using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FluentAssertions;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Voron;
using Xunit;
using Xunit.Extensions;
using Rhino.Raft.Commands;

namespace Rhino.Raft.Tests
{
	public class RaftTests
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
			try
			{
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
			finally
			{
				if (raftNodes != null) raftNodes.ForEach(node => node.Dispose());
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void On_many_node_network_after_leader_establishment_all_nodes_know_who_is_leader(int nodeCount)
		{
			var leaderEvent = new ManualResetEventSlim();
			var followerEvent = new CountdownEvent(nodeCount);

			List<RaftEngine> raftNodes = null;
			try
			{
				raftNodes = CreateRaftNetwork(nodeCount,messageTimeout:250).ToList();
				raftNodes.ForEach(node => node.StateChanged += state =>
				{
					if (state == RaftEngineState.Leader)
						leaderEvent.Set();
					else if (state == RaftEngineState.Follower && followerEvent.CurrentCount > 0)
						followerEvent.Signal();
				});

				Assert.True(leaderEvent.Wait(25000));
				
				var leaderNode = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
				Assert.NotNull(leaderNode);
				var currentLeader = leaderNode.CurrentLeader;
				
				followerEvent.Wait(15000); //wait until all other nodes become followers		
				var leaders = raftNodes.Select(x => x.CurrentLeader).ToList();

				Assert.True(leaders.All(x => x != null && x.Equals(currentLeader)));
			}
			finally
			{
				if (raftNodes != null) raftNodes.ForEach(node => node.Dispose());
			}
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
				Assert.True(stateChangeEvent.Wait(101)); //wait for elections to start

				stateChangeEvent.Reset();
				transport.Send("realNode", new RequestVoteResponse
				{
					Term = 1,
					VoteGranted = true
				});
				Assert.True(stateChangeEvent.Wait(50), "wait for votes to be acknowledged -> acknowledgement should happen at most within 50ms");
				Assert.Equal(RaftEngineState.Leader, node.State);
				
				//remove request vote messages
				transport.MessageQueue["fakeNode"].Take();

				var command = new DictionaryCommand.Set
				{
					Key = "Foo",
					Value = 123
				};
				var serializedCommand = node.CommandSerializer.Serialize(command);
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
		public void Leader_AppendCommand_command_is_not_resent_if_accepted_to_other_node()
		{
			var transport = new InMemoryTransport();
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode");

			using (var node = new RaftEngine(nodeOptions))
			{
				var stateChangeEvent = new ManualResetEventSlim();
				node.StateChanged += state => stateChangeEvent.Set();
				Assert.True(stateChangeEvent.Wait(101)); //wait for elections to start

				stateChangeEvent.Reset();
				transport.Send("realNode", new RequestVoteResponse
				{
					Term = 1,
					VoteGranted = true
				});
				Assert.True(stateChangeEvent.Wait(50),
					"wait for votes to be acknowledged -> acknowledgement should happen at most within 50ms");
				Assert.Equal(RaftEngineState.Leader, node.State);

				//remove request vote messages
				transport.MessageQueue["fakeNode"].Take();

				var command = new DictionaryCommand.Set
				{
					Key = "Foo",
					Value = 123
				};
				var serializedCommand = node.CommandSerializer.Serialize(command);
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
				node.CommitIndexChanged += commitIndex => appendEntriesAcknowledgedEvent.Set();

				//send verification that fakeNode received entries
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
					Key = "Foo"
				};
				var serializedCommand2 = node.CommandSerializer.Serialize(command2);
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

				Assert.True(stateChangeEvent.Wait(101)); //wait for elections to start

				stateChangeEvent.Reset();
				for (int i = 0; i < 3; i++)
				{
					transport.Send("realNode", new RequestVoteResponse
					{
						Term = 1,
						VoteGranted = true
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
			var nodeOptions = CreateNodeOptions("realNode", transport, 100, "fakeNode1", "fakeNode2",
				"fakeNode3");

			using (var node = new RaftEngine(nodeOptions))
			{
				var candidacyAnnoucementEvent = new ManualResetEventSlim();
				var stateTimeoutEvent = new ManualResetEventSlim();

				node.CandidacyAnnounced += candidacyAnnoucementEvent.Set;

				candidacyAnnoucementEvent.Wait(node.MessageTimeout + 5); //wait for elections to start
				node.StateTimeout += stateTimeoutEvent.Set;

				//clear transport queues
				for (int i = 1; i <= 3; i++)
				{
					transport.MessageQueue["fakeNode" + i] = new BlockingCollection<MessageEnvelope>();
				}				

				Assert.True(stateTimeoutEvent.Wait(node.MessageTimeout + 5));
				
				for (int i = 1; i <= 3; i++)
				{
					var requestVoteMessage = transport.MessageQueue["fakeNode" + i].Where(x => x.Message is RequestVoteRequest)
																				   .Select(x => x.Message as RequestVoteRequest)
																				   .OrderByDescending(x => x.Term)
																				   .First();

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

				stateChangeEvent.Wait(101); //wait for elections to start

				stateChangeEvent.Reset();
				for (int i = 0; i < 3; i++)
				{
					transport.Send("realNode", new RequestVoteResponse
					{
						Term = 1,
						VoteGranted = true
					});
				}

				stateChangeEvent.Wait(50); //now the node should be a leader, no checks here because this is tested for in another test

				stateChangeEvent.Reset();
				transport.Send("realNode",new AppendEntriesRequest
				{
					Entries = new[] { new LogEntry() },
					LeaderCommit = 0,
					LeaderId = "fakeNode1",
					PrevLogIndex = 0,
					PrevLogTerm = 1,
					Term = 2
				});

				Assert.True(stateChangeEvent.Wait(50),"acknolwedgement of a new leader should happen at most within 50ms (state change to leader)"); //wait for acknolwedgement of a new leader
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

				Assert.True(timeoutEvent.Wait(node.MessageTimeout + 5));
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
				Thread.Sleep(101); //let realNode timeout

				var fakeNodeMessage = transport.MessageQueue["fakeNode1"].First();
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


		private static RaftEngineOptions CreateNodeOptions(string nodeName, ITransport transport, int messageTimeout, params string[] peers)
		{
			var node1Options = new RaftEngineOptions(nodeName,
				StorageEnvironmentOptions.CreateMemoryOnly(),
				transport,
				new DictionaryStateMachine(), 
				messageTimeout)
			{
				AllPeers = peers,
				Stopwatch = Stopwatch.StartNew()
			};
			return node1Options;
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

			return nodeNames.Select(name => optionChangerFunc(CreateNodeOptions(name, transport, messageTimeout, nodeNames.Where(x => !x.Equals(name)).ToArray())))
				.Select(nodeOptions => new RaftEngine(nodeOptions))
				.ToList();
		}
	}
}
