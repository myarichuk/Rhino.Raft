using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using FluentAssertions.Events;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class SnapshotTests : RaftTestsBase
	{
		//this test is about verifying that there is no race condition in running snapshot
		[Fact]
		public void Snapshot_after_enough_command_applies_snapshot_is_applied_only_once()
		{
			var snapshotCreationEndedEvent = new ManualResetEventSlim();
			const int commandsCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(commandsCount)
				.All()
				.With(x => x.Completion = null)
				.Build()
				.ToList();
			var appliedAllCommandsEvent = new CountdownEvent(commandsCount);

			var raftNodes = CreateNodeNetwork(3).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			leader.MonitorEvents();
			leader.SnapshotCreationEnded += snapshotCreationEndedEvent.Set;
			leader.CommitIndexChanged += (old, @new) => appliedAllCommandsEvent.Signal();

			leader.MaxLogLengthBeforeCompaction = commandsCount - 3;
			commands.ForEach(leader.AppendCommand);

			Assert.True(appliedAllCommandsEvent.Wait(3000));
			Assert.True(snapshotCreationEndedEvent.Wait(3000));

			//should only raise the event once
			leader.ShouldRaise("SnapshotCreationEnded");
			leader.GetRecorderForEvent("SnapshotCreationEnded")
				  .Should().HaveCount(1);
		}

		[Fact]
		public void Snaphot_after_enough_command_applies_snapshot_is_created()
		{
			var snapshotCreationEndedEvent = new ManualResetEventSlim();
			const int commandsCount = 9;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(commandsCount)
						.All()
						.With(x => x.Completion = null)
						.Build()
						.ToList();

			var raftNodes = CreateNodeNetwork(3).ToList();
			raftNodes.ForEach(entry => entry.MaxEntriesPerRequest = 1);
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var appliedAllCommandsEvent = new CountdownEvent(commandsCount);
			leader.SnapshotCreationEnded += snapshotCreationEndedEvent.Set;

			leader.CommitApplied += cmd =>
			{
				if (cmd is DictionaryCommand.Set)
				{
					Trace.WriteLine("Commit applied --> DictionaryCommand.Set");
					appliedAllCommandsEvent.Signal();
				}
			};

			leader.MaxLogLengthBeforeCompaction = commandsCount - 4;
			Trace.WriteLine("<--- Started appending commands..");
			commands.ForEach(leader.AppendCommand);
			Trace.WriteLine("<--- Ended appending commands..");

			var millisecondsTimeout = Debugger.IsAttached ? 600000 : 4000;
			Assert.True(snapshotCreationEndedEvent.Wait(millisecondsTimeout));
			Assert.True(appliedAllCommandsEvent.Wait(millisecondsTimeout), "Not all commands were applied, there are still " + appliedAllCommandsEvent.CurrentCount + " commands left");

			var entriesAfterSnapshotCreation = leader.PersistentState.LogEntriesAfter(0).ToList();
			entriesAfterSnapshotCreation.Should().HaveCount((commandsCount + 1 /*nop command */ ) - leader.MaxLogLengthBeforeCompaction);
			entriesAfterSnapshotCreation.Should().OnlyContain(entry => entry.Index > leader.MaxLogLengthBeforeCompaction);
		}

		[Fact]
		public void When_receiving_snapshot_state_and_behavior_should_be_changed()
		{
			var transport = new InMemoryTransport();
			var node = CreateNodeWithVirtualNetwork("non-leader-node", transport, "leader-node", "another-node");
			node.MonitorEvents();

			var waitForStateChange = node.WaitForEventTask(
				(n, handler) => n.StateChanged += state => handler(),
// ReSharper disable once EventUnsubscriptionViaAnonymousDelegate
				(n, handler) => n.StateChanged -= state => handler());

			transport.Stream("non-leader-node", new InstallSnapshotRequest
			{
				Term = 1,
				LastIncludedIndex = 1,
				LastIncludedTerm = 1,
				From = "leader-node"
			}, stream => new MemoryStream(new byte[0]));

			var millisecondsTimeout = Debugger.IsAttached ? 600000 : 5000;
			Assert.True(waitForStateChange.Wait(millisecondsTimeout));
			//those asserts here is a precaution -> this definitely should not take more than 5 sec -> even 1 sec is too much for this

			node.State.Should().Be(RaftEngineState.SnapshotInstallation);
			node.ShouldRaise("SnapshotInstallationStarted"); //this event should be thrown when snapshot installation is starting
		}

		[Fact]
		public void Snapshot_receiving_behavior_will_reject_append_requests()
		{
			var transport = new InMemoryTransport();
			var node = CreateNodeWithVirtualNetwork("non-leader-node", transport, "leader-node", "another-node");

			var waitForStateChange = node.WaitForEventTask(
				(n, handler) => n.StateChanged += state => handler(),
				// ReSharper disable once EventUnsubscriptionViaAnonymousDelegate
				(n, handler) => n.StateChanged -= state => handler());

			transport.Stream("non-leader-node", new InstallSnapshotRequest
			{
				Term = 1,
				LastIncludedIndex = 1,
				LastIncludedTerm = 1,
				From = "leader-node"
			}, stream => new MemoryStream(new byte[0]));

			Assert.True(waitForStateChange.Wait(5000));
			var waitForEventLoop = node.WaitForEventTask(
					(n, handler) => n.EventsProcessed += handler,
					(n, handler) => n.EventsProcessed -= handler);
			
			transport.Send("non-leader-node", new AppendEntriesRequest
			{
				From = "leader-node",
				LeaderId = "leader-node",
				Term = 1,
				Entries = new LogEntry[0],
				LeaderCommit = 1,
				PrevLogIndex = 0,
				PrevLogTerm = 0
			});

			waitForEventLoop.Wait();
			transport.MessageQueue["leader-node"]
				.Should().Contain(envelope =>
					envelope.Message is AppendEntriesResponse &&
					((AppendEntriesResponse) envelope.Message).Success == false &&
					((AppendEntriesResponse) envelope.Message).Message.Equals("I am in the process of receiving a snapshot, so I cannot accept new entries at the moment"));
		}

		[Fact]
		public void New_node_should_receive_snapshot()
		{
			const int commandCount = 5;
			const int nodeCount = 3;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(commandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();			

			var nodes = CreateNodeNetwork(nodeCount).ToList();
			nodes.First().WaitForLeader();
			
			var leader = nodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			leader.MaxLogLengthBeforeCompaction = commandCount - 1;
			var allCommandsApplied = new CountdownEvent(commandCount);
			leader.CommitApplied += cmd =>
			{
				if (cmd is DictionaryCommand.Set)
					allCommandsApplied.Signal();
			};
			var waitForSnapshotCreationEnd = leader.WaitForEventTask(
				(n, handler) => n.SnapshotCreationEnded += handler,
				(n, handler) => n.SnapshotCreationEnded -= handler);

// ReSharper disable once HeapView.DelegateAllocation
			commands.ForEach(leader.AppendCommand);
			Assert.True(allCommandsApplied.Wait(5000)); //precaution -> should not take more time than this
			Assert.True(waitForSnapshotCreationEnd.Wait(5000));

			using (var newNode = new RaftEngine(CreateNodeOptions("new", leader.Transport, 1500)))
			{
				//TODO: not finished yet --> finish
			}
		}
	}
}
