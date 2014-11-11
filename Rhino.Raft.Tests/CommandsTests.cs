using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rhino.Raft.Messages;
using Xunit;
using Xunit.Extensions;

namespace Rhino.Raft.Tests
{
	public class CommandsTests : RaftTestsBase
	{
		[Fact]
		public void When_command_committed_CompletionTaskSource_is_notified()
		{
			const int CommandCount = 5;
			var raftNodes = CreateNodeNetwork(3).ToList();
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			raftNodes.First().WaitForLeader();
			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();

			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				//CommandCount + 1 --> take into account NOP command that leader sends after election
				if (newIndex == CommandCount + 1)
					commitsAppliedEvent.Set();
			};

			commands.ForEach(leader.AppendCommand);

			Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.MessageTimeout * 2));
			commands.Should().OnlyContain(cmd => cmd.Completion.Task.Status == TaskStatus.RanToCompletion);
		}

		//this test is a show-case of how to check for command commit time-out
		[Fact]
		public void While_command_not_committed_CompletionTaskSource_is_not_notified()
		{
			const int CommandCount = 5;
			var dataTransport = new InMemoryTransport();
			var raftNodes = CreateNodeNetwork(3, dataTransport).ToList();
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			raftNodes.First().WaitForLeader();
			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();

			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				//essentially fire event for (CommandCount - 1) + Nop command
				if (newIndex == CommandCount)
					commitsAppliedEvent.Set();
			};

			//don't append the last command yet
			commands.Take(CommandCount - 1).ToList().ForEach(leader.AppendCommand);
			//make sure commands that were appended before network leader disconnection are replicated
			Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.MessageTimeout * 3));

			dataTransport.DisconnectNode(leader.Name);

			var lastCommand = commands.Last();
			var commandCompletionTask = lastCommand.Completion.Task;
			var timeout = Task.Delay(leader.MessageTimeout);

			leader.AppendCommand(lastCommand);

		    var whenAnyTask = Task.WhenAny(commandCompletionTask, timeout);
			whenAnyTask.Wait();
			Assert.Equal(timeout, whenAnyTask.Result);
		}

		[Fact]
		public void Leader_AppendCommand_command_is_distributed_to_other_node()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode");

			using (var node = new RaftEngine(nodeOptions))
			{
				var stateChangeEvent = new ManualResetEventSlim();
				node.StateChanged += state => stateChangeEvent.Set();
				stateChangeEvent.Wait(); //wait for elections to start

				stateChangeEvent.Reset();
				transport.Send("realNode", new RequestVoteResponse
				{
					Term = 1,
					VoteGranted = true,
					From = "fakeNode"
				});
				Assert.True(stateChangeEvent.Wait(50), "wait for votes to be acknowledged -> acknowledgement should happen at most within 50ms");
				Assert.Equal(RaftEngineState.Leader, node.State);

				//remove request vote messages
				transport.MessageQueue["fakeNode"].Take();

				var command = new DictionaryCommand.Set
				{
					Key = "Foo",
					Value = 123,
					AssignedIndex = 2 //NOP command is in the 1st index
				};
				var serializedCommand = node.PersistentState.CommandSerializer.Serialize(command);
				var entriesAppendedEvent = new ManualResetEventSlim();
				node.EntriesAppended += entries =>
				{
					if (entries.Any(x => AreEqual(x.Data, serializedCommand)))
						entriesAppendedEvent.Set();
				};
				node.AppendCommand(command);

				entriesAppendedEvent.Wait(); //wait until the entries are appended and sent to transport

				var appendEntriesCommand = transport.MessageQueue["fakeNode"].Last().Message as AppendEntriesRequest;
				Assert.NotNull(appendEntriesCommand);
				Assert.Equal(1, appendEntriesCommand.Entries.Count(x => AreEqual(x.Data, serializedCommand)));
			}
		}

		[Fact]
		public void Leader_AppendCommand_command_is_not_resent_if_committed()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode");

			using (var node = new RaftEngine(nodeOptions))
			{
				var stateChangeEvent = new ManualResetEventSlim();
				node.StateChanged += state => stateChangeEvent.Set();
				stateChangeEvent.Wait(); //wait for elections to start

				stateChangeEvent.Reset();
				transport.Send("realNode", new RequestVoteResponse
				{
					Term = 1,
					VoteGranted = true,
					From = "fakeNode"
				});
				Assert.True(stateChangeEvent.Wait(50),
					"wait for votes to be acknowledged -> acknowledgement should happen at most within 50ms");
				Assert.Equal(RaftEngineState.Leader, node.State);

				//remove request vote messages
				transport.MessageQueue["fakeNode"].Take();

				var command = new DictionaryCommand.Set
				{
					Key = "Foo",
					Value = 123,
					AssignedIndex = 2
				};
				var serializedCommand = node.PersistentState.CommandSerializer.Serialize(command);
				var entriesAppendedEvent = new ManualResetEventSlim();
				node.EntriesAppended += entries =>
				{
					if (entries.Any(x => AreEqual(x.Data, serializedCommand)))
						entriesAppendedEvent.Set();
				};
				node.AppendCommand(command);

				entriesAppendedEvent.Wait(); //wait until the entries are appended and sent to transport
				var appendEntriesCommand = transport.MessageQueue["fakeNode"].Last().Message as AppendEntriesRequest;

				var appendEntriesAcknowledgedEvent = new ManualResetEventSlim();
				node.CommitIndexChanged += (oldCommitIndex, newCommitIndex) => appendEntriesAcknowledgedEvent.Set();

				//send verification that fakeNode received entries --> essentially do the commit
				var appendEntriesResponse = new AppendEntriesResponse
				{
					CurrentTerm = 1,
					// ReSharper disable once PossibleNullReferenceException
					LastLogIndex = appendEntriesCommand.Entries.Max(x => x.Index),
					LeaderId = "realNode",
					Success = true,
					Source = "fakeNode"
				};
				transport.Send("realNode", appendEntriesResponse);

				appendEntriesAcknowledgedEvent.Wait();

				entriesAppendedEvent.Reset();
				var command2 = new DictionaryCommand.Del
				{
					Key = "Foo",
					AssignedIndex = 3
				};
				var serializedCommand2 = node.PersistentState.CommandSerializer.Serialize(command2);
				node.EntriesAppended += entries =>
				{
					if (entries.Any(x => AreEqual(x.Data, serializedCommand2)))
						entriesAppendedEvent.Set();
				};

				node.AppendCommand(command2);
				entriesAppendedEvent.Wait(); //wait until the entries are appended and sent to transport

				appendEntriesCommand = transport.MessageQueue["fakeNode"].Last().Message as AppendEntriesRequest;
				Assert.NotNull(appendEntriesCommand);

				//make sure that after commit, only new entries are sent
				Assert.Equal(1, appendEntriesCommand.Entries.Count(x => AreEqual(x.Data, serializedCommand2)));
				Assert.Equal(0, appendEntriesCommand.Entries.Count(x => AreEqual(x.Data, serializedCommand)));
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		public void Leader_AppendCommand_for_first_time_should_distribute_commands_between_nodes(int nodeCount)
		{
			const int CommandCount = 5;
			var commandsToDistribute = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = null)
				.Build()
				.ToList();

			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 2000).ToList();
			var entriesAppended = new Dictionary<string, List<LogEntry>>();
			raftNodes.ForEach(node =>
			{
				entriesAppended.Add(node.Name, new List<LogEntry>());
				node.EntriesAppended += logEntries => entriesAppended[node.Name].AddRange(logEntries);
			});

			// ReSharper disable once PossibleNullReferenceException
			var first = raftNodes.First();
			first.WaitForLeader();
			Trace.WriteLine("<!Selected leader, proceeding with the test!>");

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();
			if (nonLeaderNode.CommitIndex == CommandCount + 1) //precaution
				commitsAppliedEvent.Set();
			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				//CommandCount + 1 --> take into account NOP command that leader sends after election
				if (newIndex == CommandCount + 1)
					commitsAppliedEvent.Set();
			};

			commandsToDistribute.ForEach(leader.AppendCommand);

			var millisecondsTimeout = 10000 * nodeCount;
			Assert.True(commitsAppliedEvent.Wait(millisecondsTimeout), "within " + millisecondsTimeout + " sec. non leader node should have all relevant commands committed");
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(10)]
		public void Leader_AppendCommand_several_times_should_distribute_commands_between_nodes(int nodeCount)
		{
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount * 2)
				.All()
				.With(x => x.Completion = null)
				.Build()
				.ToList();

			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 10000).ToList();
			var entriesAppended = new Dictionary<string, List<LogEntry>>();
			raftNodes.ForEach(node =>
			{
				entriesAppended.Add(node.Name, new List<LogEntry>());
				node.EntriesAppended += logEntries => entriesAppended[node.Name].AddRange(logEntries);
			});

			// ReSharper disable once PossibleNullReferenceException
			var first = raftNodes.First();
			first.WaitForLeader();
			Trace.WriteLine("<!Selected leader, proceeding with the test!>");

			var leader = raftNodes.First(x => x.State == RaftEngineState.Leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();
			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex >= (CommandCount * 2 + 1))
					commitsAppliedEvent.Set();
			};

			commands.Take(CommandCount).ToList().ForEach(leader.AppendCommand);
			first.WaitForLeader();
			Trace.WriteLine("<!made sure the leader is still selected, proceeding with the test!>");

			leader = raftNodes.First(x => x.State == RaftEngineState.Leader);
			commands.Skip(CommandCount).ToList().ForEach(leader.AppendCommand);

			var millisecondsTimeout = 10000 * nodeCount;
			Assert.True(commitsAppliedEvent.Wait(millisecondsTimeout), "within " + millisecondsTimeout + " sec. non leader node should have all relevant commands committed");
			Assert.Equal(CommandCount * 2 + 1, nonLeaderNode.CommitIndex);

			var committedCommands = nonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					.OfType<DictionaryCommand.Set>().ToList();
			for (int i = 0; i < 10; i++)
			{
				Assert.Equal(commands[i].Value, committedCommands[i].Value);
				Assert.Equal(commands[i].AssignedIndex, committedCommands[i].AssignedIndex);
			}
		}
	}
}
