using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using FluentAssertions.Events;
using Newtonsoft.Json;
using Rhino.Raft.Messages;
using Xunit;
using Xunit.Extensions;

namespace Rhino.Raft.Tests
{
	public class SnapshotTests : RaftTestsBase
	{

		[Theory]
		//[InlineData(1)]
		[InlineData(2)]
		//[InlineData(3)]
		//[InlineData(5)]
		//[InlineData(7)]
		public void AfterSnapshotInstalled_CanContinueGettingLogEntriesNormally(int amount)
		{
			var leader = CreateNetworkAndWaitForLeader(amount);
			leader.MaxLogLengthBeforeCompaction = 5;
			var snapshot = WaitForSnapshot(leader);
			var commits = WaitForCommitsOnCluster(
				machine => machine.Data.Count == 5);
			for (int i = 0; i < 5; i++)
			{
				leader.AppendCommand(new DictionaryCommand.Set
				{
					Key = i.ToString(),
					Value = i
				});
			}
			snapshot.Wait();
			commits.Wait();

			Assert.NotNull(leader.StateMachine.GetSnapshotWriter());

			var newNode = NewNodeFor(leader);

			leader.AddToClusterAsync(newNode.Name).Wait();

			WaitForSnapshotInstallation(newNode).Wait();

			Assert.Equal(newNode.CurrentLeader, leader.Name);

			var commit = WaitForCommit<DictionaryCommand.Set>(newNode,
				machine => machine.Data.ContainsKey("c"));

			leader.AppendCommand(new DictionaryCommand.Set
			{
				Key = "c",
				Value = 1
			});

			commit.Wait();

			var dictionary = ((DictionaryStateMachine)newNode.StateMachine).Data;
			for (int i = 0; i < 5; i++)
			{
				Assert.Equal(i, dictionary[i.ToString()]);
			}
			Assert.Equal(1, dictionary["c"]);
		}

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

			var leader = CreateNetworkAndWaitForLeader(3);

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
	}
}
