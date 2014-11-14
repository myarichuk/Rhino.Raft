using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;
using Voron;
using Xunit;
using Xunit.Extensions;

namespace Rhino.Raft.Tests
{
	public class ElectionRelatedTests : RaftTestsBase
	{
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
			var transport = new InMemoryTransport();

			var node1Options = CreateNodeOptions("node1", transport, 1000, "node1", "node2");
			var node2Options = CreateNodeOptions("node2", transport, 20000, "node1", "node2");

			using (var raftNode1 = new RaftEngine(node1Options))
			using (var raftNode2 = new RaftEngine(node2Options))
			{
				//less election timeout --> will send vote request sooner, and thus expected to become candidate first
				raftNode1.WaitForLeader();

				Assert.Equal(RaftEngineState.Leader,raftNode1.State);
				Assert.Equal(RaftEngineState.Follower, raftNode2.State);
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(10)]
		public void On_many_node_network_can_be_only_one_leader(int nodeCount)
		{
			var leaderEvent = new ManualResetEventSlim();

			var sp = Stopwatch.StartNew();
			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 100, optionChangerFunc: options =>
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


		//TODO : test with tryouts, seems to fail - very rare, but still it is there
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

				Assert.True(commands.Count> 0 && commands.Count < 3, 
					"if there are too much or too little commands that means there is a bug"); //precaution

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

			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 1500, transport: transport).ToList();
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

			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 1500, transport: transport).ToList();

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
			raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 250).ToList();
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
				var electionStartedEvent = new ManualResetEventSlim();

				node.ElectionStarted += candidacyAnnoucementEvent.Set;
				
				candidacyAnnoucementEvent.Wait(); //wait for elections to start
				//clear transport queues
				for (int i = 1; i <= 3; i++)
				{
					transport.MessageQueue["fakeNode" + i] = new BlockingCollection<MessageEnvelope>();
				}
				
				node.StateTimeout += stateTimeoutEvent.Set;
				node.ElectionStarted += electionStartedEvent.Set;
				Assert.True(stateTimeoutEvent.Wait(node.MessageTimeout + 15));
				Assert.True(electionStartedEvent.Wait(node.MessageTimeout + 15));

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

		[Fact]
		public void Request_vote_when_leader_exists_will_be_rejected()
		{
			var transport = new InMemoryTransport();
			var node = CreateNodeWithVirtualNetworkAndMakeItLeader("node",transport, "other_node1", "other_node2");

			node.State.Should().Be(RaftEngineState.Leader);

			var waitForEventLoopToProcess = node.WaitForEventTask((n, handler) => n.EventsProcessed += handler);

			transport.Send("node", new RequestVoteRequest
			{
				CandidateId = "other_node1",
				From = "other_node1",
				LastLogIndex = 0,
				Term = 0,
				LastLogTerm = 0
			});

			waitForEventLoopToProcess.Wait();

			transport.MessageQueue["other_node1"].Should().Contain(envelope => envelope.Message is RequestVoteResponse &&
																		((RequestVoteResponse)envelope.Message).VoteGranted == false &&
																		((RequestVoteResponse)envelope.Message).Message.Contains("I currently have a leader and I am receiving"));
		}
	}
}
