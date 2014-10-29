using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;
using Voron;
using Xunit;
using Xunit.Extensions;
using Rhino.Raft.Commands;

namespace Rhino.Raft.Tests
{
	public class RaftTests : IDisposable
	{
		private readonly List<RaftEngine> _nodes = new List<RaftEngine>();

		[Fact]
		public void Follower_as_a_single_node_becomes_leader_automatically()
		{
			var raftEngineOptions = new RaftEngineOptions(
				"node1",
				StorageEnvironmentOptions.CreateMemoryOnly(),
				new InMemoryTransport(),
				new DictionaryStateMachine(),
				1000);
			using (var raftNode = new RaftEngine(raftEngineOptions))
			{
				Assert.Equal(RaftEngineState.Leader, raftNode.State);
			}
		}


		[Fact]
		public void On_two_node_network_first_to_become_candidate_becomes_leader()
		{
			var candidateChangeEvent = new CountdownEvent(2);
			var transport = new InMemoryTransport();

			var node1Options = CreateNodeOptions("node1", transport, 1000, "node2");
			var node2Options = CreateNodeOptions("node2", transport, 20000, "node1");

			using (var raftNode1 = new RaftEngine(node1Options))
			using (var raftNode2 = new RaftEngine(node2Options))
			{
				//less election timeout --> will send vote request sooner, and thus expected to become candidate first
				raftNode1.StateChanged += state =>
				{
					candidateChangeEvent.Signal();

					switch (candidateChangeEvent.CurrentCount)
					{
						case 1:
							Assert.Equal(RaftEngineState.Candidate, raftNode1.State);
							break;
						case 0:
							Assert.Equal(RaftEngineState.Leader, raftNode1.State);
							Assert.Equal(RaftEngineState.Follower, raftNode2.State); 
							break;
					}
				};

				candidateChangeEvent.Wait();
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		[InlineData(10)]
		public void On_many_node_network_can_be_only_one_leader(int nodeCount)
		{
			var leaderEvent = new ManualResetEventSlim();

			List<RaftEngine> raftNodes = null;
			var sp = Stopwatch.StartNew();
			raftNodes = CreateRaftNetwork(nodeCount, messageTimeout: 100, optionChangerFunc: options =>
			{
				options.Stopwatch = sp;
				return options;
			}).ToList();
			raftNodes.ForEach(node => node.StateChanged += state =>
			{
				if (state == RaftEngineState.Leader)
				{
					leaderEvent.Set();
				}
			});

			Assert.True(leaderEvent.Wait(25000));
			Assert.Equal(1, raftNodes.Count(node => node.State == RaftEngineState.Leader));
		}

		[Fact]
		public void When_command_committed_CompletionTaskSource_is_notified()
		{
			const int CommandCount = 5;
			var raftNodes = CreateRaftNetwork(3).ToList();
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
		public async Task While_command_not_committed_CompletionTaskSource_is_not_notified()
		{
			const int CommandCount = 5;
			var dataTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(3,dataTransport).ToList();
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
			Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.MessageTimeout * 2));

			dataTransport.DisconnectNode(leader.Name);
			
			var lastCommand = commands.Last();
			var commandCompletionTask = lastCommand.Completion.Task;
			var timeout = Task.Delay(leader.MessageTimeout);
			
			leader.AppendCommand(lastCommand);

			var result = await Task.WhenAny(commandCompletionTask, timeout);
			Assert.Equal(timeout, result);
		}

		[Fact]
		public void Network_partition_should_cause_message_resend()
		{
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(4)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("leader", transport, 1000, "fake1");

			using (var leader = new RaftEngine(nodeOptions))
			{
				var stateChangeEvent = new ManualResetEventSlim();
				leader.StateChanged += state => stateChangeEvent.Set();

				stateChangeEvent.Wait(); //wait for elections to start

				stateChangeEvent.Reset();

				transport.Send("leader", new RequestVoteResponse
				{
					Term = 1,
					VoteGranted = true,
					From = "fake1"
				});
				
				Trace.WriteLine("<ack for NOP command sent>");
				Thread.Sleep(150);

				Assert.True(stateChangeEvent.Wait(50),
					"wait for votes to be acknowledged -> acknowledgement should happen at most within 50ms");

				//ack for NOP command reaching the fake1 node
				transport.Send("leader", new AppendEntriesResponse
				{
					CurrentTerm = 1,
					LastLogIndex = 1,
					LeaderId = "leader",
					Success = true,
					Source = "fake1",
					From = "fake1"
				});

				leader.AppendCommand(commands[0]);
				transport.Send("leader", new AppendEntriesResponse
				{
					CurrentTerm = 1,
					LastLogIndex = 2,
					LeaderId = "leader",
					Success = true,
					Source = "fake1",
					From = "fake1"
				});
				
				//make sure the command is committed (quorum)
				Thread.Sleep(150);
				Assert.Equal(2, leader.CommitIndex);

				//"clear" the message queue for fake1 node
				transport.MessageQueue["fake1"] = new BlockingCollection<MessageEnvelope>();
				leader.AppendCommand(commands[1]);
				leader.AppendCommand(commands[2]);
				transport.Send("leader", new AppendEntriesResponse
				{
					CurrentTerm = 1,
					LeaderId = "leader",
					Success = false,
					Source = "fake1"
				});

				Thread.Sleep(150);

				var commandResendingMessage = transport.MessageQueue["fake1"].Where(x => x.Message is AppendEntriesRequest)
																			 .Select(x => (AppendEntriesRequest)x.Message)
																			 .FirstOrDefault(x => x.Entries.Length > 0);
				Assert.NotNull(commandResendingMessage);
				var deserializedCommands =
					commandResendingMessage.Entries.Select(x => leader.PersistentState.CommandSerializer.Deserialize(x.Data) as DictionaryCommand.Set)
												   .ToList();

				for (int i = 1; i < 3; i++)
				{
					Assert.Equal(commands[i].Value, deserializedCommands[i].Value);
					Assert.Equal(commands[i].AssignedIndex, deserializedCommands[i].AssignedIndex);
				}
			}
		}


		/*
		 * This test deals with network "partition" -> leader is detached from the rest of the nodes (simulation of network issues)
		 * Before the network is partitioned the leader distributes the first three commands, then the partition happens.
		 * Then the detached leader has 2 more commands appended - but because of network partition, they are not distributed to other nodes
		 * When communication is restored, the leader from before becomes follower, and the new leader makes roll back on log of former leader, 
		 * so only the first three commands are in the log of former leader node
		 */
		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Network_partition_for_more_time_than_timeout_can_be_healed(int nodeCount)
		{
			var transport = new InMemoryTransport();
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var raftNodes = CreateRaftNetwork(nodeCount, messageTimeout: 1500, transport: transport).ToList();
			raftNodes.First().WaitForLeader();
			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();
			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex == 4) //index == 4 --> NOP command + first three commands
					commitsAppliedEvent.Set();
			};

			commands.Take(3).ToList().ForEach(leader.AppendCommand);
			Assert.True(commitsAppliedEvent.Wait(5000)); //with in-memory transport it shouldn't take more than 5 sec

			Trace.WriteLine("<Disconnecting leader!> (" + leader.Name + ")");
			transport.DisconnectNode(leader.Name);

			commands.Skip(3).ToList().ForEach(leader.AppendCommand);
			var formerLeader = leader;
			Thread.Sleep(raftNodes.Max(x => x.MessageTimeout) + 5); // cause election while current leader is disconnected

			Trace.WriteLine("<Reconnecting leader!> (" + leader.Name + ")");
			transport.ReconnectNode(leader.Name);

			//other leader was selected
			raftNodes.First().WaitForLeader();
			leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);			
			Assert.NotNull(leader);
			
			//former leader that is now a follower, should get the first 3 entries it distributed
			while (formerLeader.CommitIndex < 4) //CommitIndex == 4 --> NOP command + first three commands
				Thread.Sleep(50);

			Assert.True(formerLeader.CommitIndex == 4);
			var committedCommands = formerLeader.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					.OfType<DictionaryCommand.Set>()
																					.ToList();
			for (int i = 0; i < 3; i++)
			{
				commands[i].Value.Should().Be(committedCommands[i].Value);
				commands[i].AssignedIndex.Should().Be(committedCommands[i].AssignedIndex);
			}
		}
		
		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Network_partition_for_less_time_than_timeout_can_be_healed_without_elections(int nodeCount)
		{
			var transport = new InMemoryTransport();
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var raftNodes = CreateRaftNetwork(nodeCount, messageTimeout: 1500, transport: transport).ToList();

			raftNodes.First().WaitForLeader();
			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();

			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex == CommandCount + 1)
					commitsAppliedEvent.Set();
			};

			commands.Take(CommandCount - 1).ToList().ForEach(leader.AppendCommand);
			while(nonLeaderNode.CommitIndex < 2) //make sure at least one command is committed
				Thread.Sleep(50);

			Trace.WriteLine("<Disconnecting leader!> (" + leader.Name + ")");
			transport.DisconnectNode(leader.Name);

			leader.AppendCommand(commands.Last());
			
			Trace.WriteLine("<Reconnecting leader!> (" + leader.Name + ")");			
			transport.ReconnectNode(leader.Name);
			Assert.Equal(RaftEngineState.Leader, leader.State);

			Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.MessageTimeout * nodeCount));

			var committedCommands = nonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					.OfType<DictionaryCommand.Set>()
																					.ToList();
			for (int i = 0; i < CommandCount; i++)
			{
				commands[i].Value.Should().Be(committedCommands[i].Value);
				commands[i].AssignedIndex.Should().Be(committedCommands[i].AssignedIndex);
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		public void On_many_node_network_after_leader_establishment_all_nodes_know_who_is_leader(int nodeCount)
		{
			var followerEvent = new CountdownEvent(nodeCount);

			List<RaftEngine> raftNodes = null;
			raftNodes = CreateRaftNetwork(nodeCount, messageTimeout: 250).ToList();
			raftNodes.ForEach(node => node.StateChanged += state =>
			{
				if (state == RaftEngineState.Follower && followerEvent.CurrentCount > 0)
					followerEvent.Signal();
			});

			raftNodes.First().WaitForLeader();

			var leaderNode = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leaderNode);
			var currentLeader = leaderNode.CurrentLeader;

			followerEvent.Wait(15000); //wait until all other nodes become followers		
			var leadersOfNodes = raftNodes.Select(x => x.CurrentLeader).ToList();

			leadersOfNodes.Should().NotContainNulls("After leader is established, all nodes should know that leader exists");
			leadersOfNodes.Should().OnlyContain(l => l.Equals(currentLeader, StringComparison.InvariantCultureIgnoreCase),
				"after leader establishment, all nodes should know only one, selected leader");
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
					if(entries.Any(x => AreEqual(x.Data,serializedCommand)))
						entriesAppendedEvent.Set();
				};
				node.AppendCommand(command);

				entriesAppendedEvent.Wait(); //wait until the entries are appended and sent to transport

				var appendEntriesCommand = transport.MessageQueue["fakeNode"].Last().Message as AppendEntriesRequest;
				Assert.NotNull(appendEntriesCommand);
				Assert.Equal(1,appendEntriesCommand.Entries.Count(x => AreEqual(x.Data,serializedCommand)));
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
				node.CommitIndexChanged += (oldCommitIndex,newCommitIndex) => appendEntriesAcknowledgedEvent.Set();

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

			var raftNodes = CreateRaftNetwork(nodeCount, messageTimeout: 2000).ToList();
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
				if(newIndex == CommandCount + 1)
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

			var raftNodes = CreateRaftNetwork(nodeCount, messageTimeout: 10000).ToList();
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
			Assert.Equal(CommandCount*2 + 1, nonLeaderNode.CommitIndex);

			var committedCommands = nonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					.OfType<DictionaryCommand.Set>().ToList();
			for (int i = 0; i < 10; i++)
			{
				Assert.Equal(commands[i].Value, committedCommands[i].Value);
				Assert.Equal(commands[i].AssignedIndex, committedCommands[i].AssignedIndex);
			}
		}

		[Fact]
		public void Removal_of_non_existing_node_should_throw()
		{
			using (var node = new RaftEngine(CreateNodeOptions("node", new InMemoryTransport(), 1500, "node")))
			{
				node.Invoking(x => x.RemoveFromClusterAsync("non-existing"))
					.ShouldThrow<InvalidOperationException>();
			}

		}

		[Fact]
		public void Adding_already_existing_node_should_throw()
		{
			using (var node = new RaftEngine(CreateNodeOptions("node", new InMemoryTransport(), 1500, "node", "other-node")))
			{
				node.Invoking(x => x.AddToClusterAsync("other-node"))
					.ShouldThrow<InvalidOperationException>();
			}
		}

		[Fact]
		public void Candidate_with_enough_votes_becomes_leader()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode1", "fakeNode2",
				"fakeNode3");

			using (var node = new RaftEngine(nodeOptions))
			{
				var stateChangeEvent = new ManualResetEventSlim();
				node.StateChanged += state => stateChangeEvent.Set();

				stateChangeEvent.Wait(); //wait for elections to start

				stateChangeEvent.Reset();
				for (int i = 0; i < 3; i++)
				{
					transport.Send("realNode", new RequestVoteResponse
					{
						Term = 1,
						VoteGranted = true,
						From = "fakeNode" + i
					});
				}

				Assert.True(stateChangeEvent.Wait(50), "wait for votes to be acknowledged -> acknowledgement should happen at most within 50ms");
				Assert.Equal(RaftEngineState.Leader, node.State);
			}
		}

		[Fact]
		public void Candidate_with_split_vote_should_restart_elections_on_timeout()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 1000, "fakeNode1", "fakeNode2",
				"fakeNode3");

			using (var node = new RaftEngine(nodeOptions))
			{
				var candidacyAnnoucementEvent = new ManualResetEventSlim();
				var stateTimeoutEvent = new ManualResetEventSlim();

				node.ElectionStarted += candidacyAnnoucementEvent.Set;
				
				candidacyAnnoucementEvent.Wait(); //wait for elections to start
				//clear transport queues
				for (int i = 1; i <= 3; i++)
				{
					transport.MessageQueue["fakeNode" + i] = new BlockingCollection<MessageEnvelope>();
				}
				
				node.StateTimeout += stateTimeoutEvent.Set;


				Assert.True(stateTimeoutEvent.Wait(node.MessageTimeout + 15));
				
				for (int i = 1; i <= 3; i++)
				{
					var requestVoteMessage = transport.MessageQueue["fakeNode" + i].Where(x => x.Message is RequestVoteRequest)
																				   .Select(x => x.Message as RequestVoteRequest)
																				   .OrderByDescending(x => x.Term)
																				   .FirstOrDefault();
					Assert.NotNull(requestVoteMessage);
					Assert.Equal("realNode", requestVoteMessage.CandidateId);			
					Assert.Equal(2,requestVoteMessage.Term);
				}
			}
		}

		[Fact]
		public void Leader_should_step_down_if_another_leader_is_up()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode1", "fakeNode2",
				"fakeNode3");

			using (var node = new RaftEngine(nodeOptions))
			{
				var stateChangeEvent = new ManualResetEventSlim();
				node.StateChanged += state => stateChangeEvent.Set();

				stateChangeEvent.Wait(); //wait for elections to start

				stateChangeEvent.Reset();
				for (int i = 1; i <= 3; i++)
				{
					transport.Send("realNode", new RequestVoteResponse
					{
						Term = 1,
						VoteGranted = true,
						From = "fakeNode" + i
					});
				}

				stateChangeEvent.Wait(150); //now the node should be a leader, no checks here because this is tested for in another test

				stateChangeEvent.Reset();
				transport.Send("realNode",new AppendEntriesRequest
				{
					Entries = new[] { new LogEntry() },
					LeaderCommit = 0,
					LeaderId = "fakeNode1",
					PrevLogIndex = 0,
					PrevLogTerm = 1,
					Term = 2,
					From = "fakeNode1"
				});

				Assert.True(stateChangeEvent.Wait(150),"acknolwedgement of a new leader should happen at most within 150ms (state change to leader)"); //wait for acknolwedgement of a new leader
				Assert.Equal(RaftEngineState.Follower, node.State);
			}
		}

		[Fact]
		public void Follower_on_timeout_should_become_candidate()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode");

			using (var node = new RaftEngine(nodeOptions))
			{
				var timeoutEvent = new ManualResetEventSlim();
				node.StateTimeout += timeoutEvent.Set;

				timeoutEvent.Wait();
				Assert.Equal(RaftEngineState.Candidate,node.State);
			}
		}

		[Fact]
		public void Follower_on_timeout_should_send_vote_requests()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode1","fakeNode2");

			using (var node = new RaftEngine(nodeOptions))
			{
				var electionEvent = new ManualResetEventSlim();
				node.ElectionStarted += electionEvent.Set;
				
				electionEvent.Wait();
				
				Trace.WriteLine("<Finished realNode timeout>");

				var fakeNodeMessage = transport.MessageQueue["fakeNode1"].FirstOrDefault();
				Assert.NotNull(fakeNodeMessage);
				Assert.IsType<RequestVoteRequest>(fakeNodeMessage.Message);
				var requestVoteMessage = fakeNodeMessage.Message as RequestVoteRequest;
				Assert.Equal("realNode", requestVoteMessage.CandidateId);

				Assert.Equal(1, transport.MessageQueue["fakeNode2"].Count);
				
				fakeNodeMessage = transport.MessageQueue["fakeNode2"].First();
				Assert.IsType<RequestVoteRequest>(fakeNodeMessage.Message);
				requestVoteMessage = fakeNodeMessage.Message as RequestVoteRequest;
				Assert.Equal("realNode", requestVoteMessage.CandidateId);
			}
		}



		[Fact]
		public void AllPeers_and_AllVotingPeers_can_be_persistantly_saved_and_loaded()
		{
			var cancellationTokenSource = new CancellationTokenSource();

			var path = "test" + Guid.NewGuid();
			try
			{
				var expectedAllVotingPeers = new List<string> { "Node123", "Node1", "Node2", "NodeG", "NodeB", "NodeABC" };

				using (var options = StorageEnvironmentOptions.ForPath(path))
				{
					using (var persistentState = new PersistentState(options, cancellationTokenSource.Token)
					{
						CommandSerializer = new JsonCommandSerializer()
					})
					{
						var currentConfiguration = persistentState.GetCurrentConfiguration();
						Assert.Empty(currentConfiguration.AllVotingNodes);

						persistentState.SetCurrentTopology(new Topology(expectedAllVotingPeers),null);
					}
				}
				using (var options = StorageEnvironmentOptions.ForPath(path))
				{
					using (var persistentState = new PersistentState(options, cancellationTokenSource.Token)
					{
						CommandSerializer = new JsonCommandSerializer()
					})
					{
						var currentConfiguration = persistentState.GetCurrentConfiguration();
						expectedAllVotingPeers.Should().Contain(currentConfiguration.AllVotingNodes);
					}
				}
			}
			finally
			{
				new DirectoryInfo(path).Delete(true);
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Follower_removed_from_cluster_modifies_member_lists_on_remaining_nodes(int nodeCount)
		{
			var raftNodes = CreateRaftNetwork(nodeCount).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(n => n.State == RaftEngineState.Leader);
			var removedNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader);
			var nonLeaderNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader && !ReferenceEquals(n,removedNode));
			Assert.NotNull(leader);
			Assert.NotNull(removedNode);

			Trace.WriteLine(string.Format("<-- Leader chosen: {0} -->", leader.Name));
			Trace.WriteLine(string.Format("<-- Node to be removed: {0} -->", removedNode.Name));

			raftNodes.Remove(removedNode);
			var topologyChangeComittedEvent = new CountdownEvent(nodeCount - 1);

			raftNodes.ForEach(node => node.TopologyChangeFinished += cmd => topologyChangeComittedEvent.Signal());

			Trace.WriteLine(string.Format("<-- Removing {0} from the cluster -->", removedNode.Name));
			leader.RemoveFromClusterAsync(removedNode.Name).Wait();

			Assert.True(topologyChangeComittedEvent.Wait(nodeCount * 2500));

			var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();
			Trace.WriteLine("<-- expectedNodeNameList:" + expectedNodeNameList.Aggregate(String.Empty,(all,curr) => all + ", " + curr));
			raftNodes.ForEach(node => node.AllVotingNodes.Should().BeEquivalentTo(expectedNodeNameList,"node " + node.Name + " should have expected AllVotingNodes list"));
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Leader_removed_from_cluster_modifies_member_lists_on_remaining_nodes(int nodeCount)
		{
			var topologyChangeComittedEvent = new CountdownEvent(nodeCount - 1);

			var raftNodes = CreateRaftNetwork(nodeCount).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(n => n.State == RaftEngineState.Leader);
			var nonLeaderNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader);
			Assert.NotNull(leader);
			Assert.NotNull(nonLeaderNode);

			Trace.WriteLine(string.Format("<-- Leader chosen: {0} -->", leader.Name));
			Trace.WriteLine(string.Format("<-- Non-leader node: {0} -->", nonLeaderNode.Name));

			raftNodes.Remove(leader);
			raftNodes.ForEach(node => node.TopologyChangeFinished += cmd => topologyChangeComittedEvent.Signal());
			leader.RemoveFromClusterAsync(leader.Name).Wait();

			Assert.True(topologyChangeComittedEvent.Wait(nodeCount * 2500));

			var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();
			Trace.WriteLine("<-- expectedNodeNameList:" + expectedNodeNameList.Aggregate(String.Empty, (all, curr) => all + ", " + curr));
			raftNodes.ForEach(node => node.AllVotingNodes.Should().BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
		}

		[Fact]
		public void Follower_removed_from_cluster_does_not_affect_leader_and_commits()
		{
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(5)
						.All()
						.With(x => x.Completion = null)
						.Build()
						.ToList();

			var raftNodes = CreateRaftNetwork(4,messageTimeout:1500).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader);
			var someCommitsAppliedEvent = new ManualResetEventSlim();
			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex >= 3) //two commands and NOP
					someCommitsAppliedEvent.Set();
			};

			leader.AppendCommand(commands[0]);
			leader.AppendCommand(commands[1]);

			Assert.True(someCommitsAppliedEvent.Wait(2000));

			Assert.Equal(3, leader.CurrentTopology.QuoromSize);
			Trace.WriteLine(string.Format("<--- Removing from cluster {0} --->", nonLeaderNode.Name));
			leader.RemoveFromClusterAsync(nonLeaderNode.Name).Wait();

			var otherNonLeaderNode = raftNodes.First(x => x.State != RaftEngineState.Leader && !ReferenceEquals(x, nonLeaderNode));
			
			var allCommitsAppliedEvent = new ManualResetEventSlim();
			var sw = Stopwatch.StartNew();
			const int indexChangeTimeout = 3000;
			otherNonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex == 7 || sw.ElapsedMilliseconds >= indexChangeTimeout) //limit waiting to 5 sec
				{
					allCommitsAppliedEvent.Set();
					sw.Stop();
				}
			};
			
			Trace.WriteLine(string.Format("<--- Appending remaining commands ---> (leader name = {0})", leader.Name));
			leader.AppendCommand(commands[2]);
			leader.AppendCommand(commands[3]);
			leader.AppendCommand(commands[4]);
			
			Assert.True(allCommitsAppliedEvent.Wait(indexChangeTimeout) && sw.ElapsedMilliseconds < indexChangeTimeout);
			
			var committedCommands = otherNonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					     .OfType<DictionaryCommand.Set>().ToList();
			committedCommands.Should().HaveCount(5);
			for (int i = 0; i < 5; i++)
			{
				Assert.Equal(commands[i].Value, committedCommands[i].Value);
				Assert.Equal(commands[i].AssignedIndex, committedCommands[i].AssignedIndex);
			}

			otherNonLeaderNode.CommitIndex.Should().Be(leader.CommitIndex,"after all commands have been comitted, on non-leader nodes should be the same commit index as on index node");
		}

		[Fact]
		public void Cluster_cannot_have_two_concurrent_node_additions()
		{
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(4, messageTimeout: 1500,transport:inMemoryTransport).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			using (var additionalNode = new RaftEngine(CreateNodeOptions("extra_node", inMemoryTransport, 1500)))
			using (var additionalNode2 = new RaftEngine(CreateNodeOptions("extra_node2", inMemoryTransport, 1500)))
			{

				leader.AddToClusterAsync(additionalNode.Name);

				//if another removal from cluster is in progress, 
				Assert.Throws<InvalidOperationException>(() => leader.AddToClusterAsync(additionalNode2.Name).Wait());
			}
		}

		[Fact]
		public void Cluster_cannot_have_two_concurrent_node_removals()
		{
			var raftNodes = CreateRaftNetwork(4, messageTimeout: 1500).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			var nonLeader = raftNodes.FirstOrDefault(x => x.State != RaftEngineState.Leader);
			Assert.NotNull(nonLeader);

			leader.RemoveFromClusterAsync(nonLeader.Name);

			//if another removal from cluster is in progress, 
			Assert.Throws<InvalidOperationException>(() => leader.RemoveFromClusterAsync(leader.Name).Wait());
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		public async Task Leader_removed_from_cluster_will_cause_new_election(int nodeCount)
		{
			var electionStartedEvent = new ManualResetEventSlim();

			var raftNodes = CreateRaftNetwork(nodeCount).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			raftNodes.ForEach(node =>
			{
				if (!ReferenceEquals(node, leader))
					node.ElectionStarted += electionStartedEvent.Set;
			});
			await leader.RemoveFromClusterAsync(leader.Name);

			Assert.True(electionStartedEvent.Wait(3000));
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Node_added_to_cluster_will_not_cause_new_election(int nodeCount)
		{
			var electionStartedEvent = new ManualResetEventSlim();
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(nodeCount, inMemoryTransport).ToList();

			using (var additionalNode = new RaftEngine(CreateNodeOptions("extra_node", inMemoryTransport, 1500)))
			{

				raftNodes.First().WaitForLeader();

				var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
				Assert.NotNull(leader);

				raftNodes.ForEach(node =>
				{
					if (!ReferenceEquals(node, leader))
						node.ElectionStarted += electionStartedEvent.Set;
				});

				leader.AddToClusterAsync(additionalNode.Name).Wait();

				Assert.False(electionStartedEvent.Wait(3000));
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Non_leader_Node_removed_from_cluster_should_update_peers_list(int nodeCount)
		{
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(nodeCount, inMemoryTransport).ToList();
			var topologyChangeTracker = new CountdownEvent(nodeCount - 1);
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.First(n => n.State == RaftEngineState.Leader);
			var nodeToRemove = raftNodes.First(n => n.State != RaftEngineState.Leader);

			var nodesThatShouldRemain = raftNodes.Where(n => ReferenceEquals(n, nodeToRemove) == false)
												 .Select(n => n.Name)
												 .ToList();

			raftNodes.Where(n => ReferenceEquals(n, nodeToRemove) == false).ToList()
					 .ForEach(node => node.TopologyChangeFinished += cmd => topologyChangeTracker.Signal());
			
			Trace.WriteLine("<--- started removing");
			leader.RemoveFromClusterAsync(nodeToRemove.Name).Wait();
			Assert.True(topologyChangeTracker.Wait(5000));

			var nodePeerLists = raftNodes.Where(n => ReferenceEquals(n, nodeToRemove) == false)					 	
										 .Select(n => n.AllVotingNodes)
										 .ToList();

			nodePeerLists.ForEach(peerList => peerList.ShouldBeEquivalentTo(nodesThatShouldRemain));
		}

		[Fact]
		public void Cluster_nodes_are_able_to_recover_after_shutdown_in_the_middle_of_topology_change()
		{
			const int timeout = 2500;
			var inMemoryTransport = new InMemoryTransport();

			var testDataFolder = Path.Combine(Directory.GetCurrentDirectory(),"test");
			if(Directory.Exists(testDataFolder))
				Directory.Delete(testDataFolder,true);

			var tempFolder = Guid.NewGuid().ToString();
			var nodeApath = Path.Combine(Directory.GetCurrentDirectory(),"test", tempFolder, "A");			
			var nodeBpath = Path.Combine(Directory.GetCurrentDirectory(),"test", tempFolder, "B");

			var topologyChangedOnAdditionalNode = new ManualResetEventSlim();

			using (var additionalNode = new RaftEngine(CreateNodeOptions("nodeC", inMemoryTransport, timeout,"nodeC")))
			{
				additionalNode.TopologyChangeFinished += cmd => topologyChangedOnAdditionalNode.Set();
				var topologyChangeStarted = new ManualResetEventSlim();
				string nonLeaderName;
				using (var nodeA = new RaftEngine(CreateNodeOptions("nodeA", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeApath), "nodeA", "nodeB")))
				using (var nodeB = new RaftEngine(CreateNodeOptions("nodeB", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeBpath), "nodeA", "nodeB")))
				{
					nodeA.WaitForLeader();
					Assert.True(nodeA.State != nodeB.State, String.Format("nodeA.State {0} != nodeB.State {1}", nodeA.State, nodeB.State));

					var leader = nodeA.State == RaftEngineState.Leader ? nodeA : nodeB;
					var nonLeader = nodeA.State != RaftEngineState.Leader ? nodeA : nodeB;

					nonLeaderName = nonLeader.Name;
										
					nonLeader.TopologyChangeStarted += () =>
					{
						topologyChangeStarted.Set();
						Console.WriteLine("<---disconnected from sending : " + nonLeaderName);
						inMemoryTransport.DisconnectNodeSending(nonLeaderName);
					};

					Console.WriteLine("<---adding new node here");
					leader.AddToClusterAsync(additionalNode.Name);
					Assert.True(topologyChangeStarted.Wait(2000));
				}
				Console.WriteLine("<---nodeA, nodeB are down");

				inMemoryTransport.ReconnectNodeSending(nonLeaderName);

				using (var nodeA = new RaftEngine(CreateNodeOptions("nodeA", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeApath), "nodeA", "nodeB")))
				using (var nodeB = new RaftEngine(CreateNodeOptions("nodeB", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeBpath), "nodeA", "nodeB")))
				{
					var topologyChangesFinished = new CountdownEvent(2);
					nodeA.TopologyChangeFinished += cmd => topologyChangesFinished.Signal();
					nodeB.TopologyChangeFinished += cmd => topologyChangesFinished.Signal();

					Console.WriteLine("<---nodeA, nodeB are up, waiting for topology change on additional nodes");
					Assert.True(topologyChangedOnAdditionalNode.Wait(15000));
					Assert.True(topologyChangesFinished.Wait(15000));

					nodeA.AllVotingNodes.Should().Contain(additionalNode.Name);
					nodeB.AllVotingNodes.Should().Contain(additionalNode.Name);
				}
			}
		}


		//TODO: test further --> might have a race condition related
		//might happen if additionalNode will have large timeout (much larger than the rest of nodes
		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Node_added_to_cluster_should_update_peers_list(int nodeCount)
		{
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(nodeCount, inMemoryTransport).ToList();
			var topologyChangeTracker = new CountdownEvent(nodeCount + 1);
			raftNodes.First().WaitForLeader();
			using (var additionalNode = new RaftEngine(CreateNodeOptions("nada0", inMemoryTransport, raftNodes.First().MessageTimeout * 4)))
			{
				var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
				Assert.NotNull(leader);

				raftNodes.ForEach(node => node.TopologyChangeFinished += cmd => topologyChangeTracker.Signal());
				additionalNode.TopologyChangeFinished += cmd => topologyChangeTracker.Signal();
				Trace.WriteLine("<------------- adding additional node to cluster");
				leader.AddToClusterAsync(additionalNode.Name).Wait();
				var millisecondsTimeout = (Debugger.IsAttached == false) ? nodeCount * 2000 : nodeCount * 50000;
				Assert.True(topologyChangeTracker.Wait(millisecondsTimeout), "topologyChangeTracker's count should be 0, but it is " + topologyChangeTracker.CurrentCount);

				raftNodes.ForEach(node => 
					node.AllVotingNodes.Should().Contain(additionalNode.Name, 
					string.Format("node with name: {0} should contain in its AllVotingNodes the newly added node's name", node.Name)));

				additionalNode.AllVotingNodes.Should().Contain(raftNodes.Select(node => node.Name));
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Leader_removed_from_cluster_will_not_disrupt_log_entry_order(int nodeCount)
		{
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(4)
				.All()
				.With(x => x.Completion = null)
				.Build()
				.ToList();

			var raftNodes = CreateRaftNetwork(nodeCount).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			var nonLeader = raftNodes.FirstOrDefault(x => x.State != RaftEngineState.Leader);
			Assert.NotNull(leader);
			Assert.NotNull(nonLeader);

			var firstTwoCommandsCommitted = new CountdownEvent(2);
			leader.CommitApplied += cmd =>
			{
				if (cmd is DictionaryCommand.Set)
					firstTwoCommandsCommitted.Signal();
			};

			leader.AppendCommand(commands[0]);
			leader.AppendCommand(commands[1]);

			Assert.True(firstTwoCommandsCommitted.Wait(2000));

			//now remove the leader and wait for the new leader
			var topologyAppliedEvent = new ManualResetEventSlim();
			nonLeader.TopologyChangeFinished += cmd => topologyAppliedEvent.Set();

			leader.RemoveFromClusterAsync(leader.Name).Wait();

			//wait for twice the default node timeout -> shouldn't take more time than that
			//Assert.True(topologyAppliedEvent.Wait(5000));
			topologyAppliedEvent.Wait(5000);
			nonLeader.WaitForLeader();
			var newLeader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader && !ReferenceEquals(x, leader));

			newLeader.Should()
				.NotBeNull("After removing leader, there should be re-election and new, _other_ leader should be selected");

			var theRestOfCommandsApplied = new CountdownEvent(2);

			// ReSharper disable once PossibleNullReferenceException
			newLeader.CommitApplied += cmd =>
			{
				if (cmd is DictionaryCommand.Set)
					theRestOfCommandsApplied.Signal();
			};

			newLeader.AppendCommand(commands[2]);
			newLeader.AppendCommand(commands[3]);

			theRestOfCommandsApplied.Wait(4000);

			var committedCommands =
				newLeader.PersistentState.LogEntriesAfter(0)
					.Select(x => nonLeader.PersistentState.CommandSerializer.Deserialize(x.Data))
					.OfType<DictionaryCommand.Set>().ToList();

			committedCommands.Should().HaveCount(4);
			for (int i = 0; i < 4; i++)
			{
				Assert.Equal(commands[i].Value, committedCommands[i].Value);
				Assert.Equal(commands[i].AssignedIndex, committedCommands[i].AssignedIndex);
			}
		}

		private static RaftEngineOptions CreateNodeOptions(string nodeName, ITransport transport, int messageTimeout, params string[] peers)
		{
			var nodeOptions = new RaftEngineOptions(nodeName,
				StorageEnvironmentOptions.CreateMemoryOnly(),
				transport,
				new DictionaryStateMachine(), 
				messageTimeout)
			{
				AllVotingNodes = peers,
				Stopwatch = Stopwatch.StartNew()
			};
			return nodeOptions;
		}

		private static RaftEngineOptions CreateNodeOptions(string nodeName, ITransport transport, int messageTimeout, StorageEnvironmentOptions storageOptions, params string[] peers)
		{
			var nodeOptions = new RaftEngineOptions(nodeName,
				storageOptions,
				transport,
				new DictionaryStateMachine(),
				messageTimeout)
			{
				AllVotingNodes = peers,
				Stopwatch = Stopwatch.StartNew()
			};
			return nodeOptions;
		}


		private bool AreEqual(byte[] array1, byte[] array2)
		{
			if (array1.Length != array2.Length)
				return false;

			return !array1.Where((t, i) => t != array2[i]).Any();
		}

		private IEnumerable<RaftEngine> CreateRaftNetwork(int nodeCount, ITransport transport = null, int messageTimeout = 1000,Func<RaftEngineOptions,RaftEngineOptions> optionChangerFunc = null)
		{
			transport = transport ?? new InMemoryTransport();
			var nodeNames = new List<string>();
			for (int i = 0; i < nodeCount; i++)
			{
				nodeNames.Add("node" + i);
			}

			if (optionChangerFunc == null)
				optionChangerFunc = options => options;

			var raftNetwork = nodeNames
				.Select(name => optionChangerFunc(CreateNodeOptions(name, transport, messageTimeout, nodeNames.ToArray())))
				.Select(nodeOptions => new RaftEngine(nodeOptions))
				.ToList();

			_nodes.AddRange(raftNetwork);

			return raftNetwork;
		}

		public void Dispose()
		{
			_nodes.ForEach(node => node.Dispose());
		}
	}
}
