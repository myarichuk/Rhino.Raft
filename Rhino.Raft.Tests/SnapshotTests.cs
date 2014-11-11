using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using FluentAssertions.Events;
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

			var raftNodes = CreateRaftNetwork(3).ToList();
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

			var raftNodes = CreateRaftNetwork(3).ToList();
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
		public void New_node_should_receive_snapshot_when_applicable()
		{
			var snapshotCreationEndedEvent = new ManualResetEventSlim();
			const int commandsCount = 9;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(commandsCount)
				.All()
				.With(x => x.Completion = null)
				.Build()
				.ToList();

			var transport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(3, transport).ToList();
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
			Assert.True(appliedAllCommandsEvent.Wait(millisecondsTimeout),
				"Not all commands were applied, there are still " + appliedAllCommandsEvent.CurrentCount + " commands left");

			using (var node = new RaftEngine(CreateNodeOptions("new_node", transport, 1500)))
			{
				var newNodeTopologyChangedEvent = new CountdownEvent(2);

				node.TopologyChangeFinished += cmd => newNodeTopologyChangedEvent.Signal();
				leader.TopologyChangeFinished += cmd => newNodeTopologyChangedEvent.Signal();
				leader.AddToClusterAsync(node.Name).Wait();

				Assert.True(newNodeTopologyChangedEvent.Wait(5000));

				
			}
		}

	}
}
