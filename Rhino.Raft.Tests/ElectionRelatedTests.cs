using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;
using Rhino.Raft.Transport;
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
			var hub = new InMemoryTransportHub();
			var raftEngineOptions = new RaftEngineOptions(
				"node1",
				StorageEnvironmentOptions.CreateMemoryOnly(),
				hub.CreateTransportFor("node1"),
				new DictionaryStateMachine()
				)
			{
				MessageTimeout = 1000
			};
			using (var raftNode = new RaftEngine(raftEngineOptions))
			{
				Assert.Equal(RaftEngineState.Leader, raftNode.State);
			}
		}

		[Fact]
		public void On_two_node_network_first_to_become_candidate_becomes_leader()
		{
			CreateNodeNetwork(2);

			ForceTimeout("node0");
			//less election timeout --> will send vote request sooner, and thus expected to become candidate first
			Nodes.First().WaitForLeader();

			Assert.Equal(RaftEngineState.Leader, Nodes.First().State);
			Assert.Equal(RaftEngineState.Follower, Nodes.Last().State);
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

		[Fact]
		public void Network_partition_should_cause_message_resend()
		{

			DisconnectNode("node1");
			DisconnectNodeSending("node1");
			DisconnectNode("node2");
			DisconnectNodeSending("node2");
			var raftNodes = CreateNodeNetwork(3, messageTimeout: 300).ToList();
			var countdown = new CountdownEvent(2);
			raftNodes[0].ElectionStarted += () =>
			{
				if (countdown.CurrentCount > 0)
					countdown.Signal();
			};

			Assert.True(countdown.Wait(1500));

			ReconnectNode("node1");
			ReconnectNodeSending("node2");
			ReconnectNode("node2");
			ReconnectNodeSending("node2");

			raftNodes.First().WaitForLeader();
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
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var leader = CreateNetworkAndWaitForLeader(nodeCount);

			var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();
			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex == 4) //index == 4 --> NOP command + first three commands
					commitsAppliedEvent.Set();
			};

			commands.Take(3).ToList().ForEach(leader.AppendCommand);
			Assert.True(commitsAppliedEvent.Wait(5000)); //with in-memory transport it shouldn't take more than 5 sec

			WriteLine("<Disconnecting leader!> (" + leader.Name + ")");
			DisconnectNode(leader.Name);

			commands.Skip(3).ToList().ForEach(leader.AppendCommand);
			var formerLeader = leader;
			Thread.Sleep(Nodes.Max(x => x.Options.MessageTimeout) + 5); // cause election while current leader is disconnected

			WriteLine("<Reconnecting leader!> (" + leader.Name + ")");
			ReconnectNode(leader.Name);

			//other leader was selected
			Nodes.First().WaitForLeader();
			leader = Nodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
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
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: 1500).ToList();

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
			while (nonLeaderNode.CommitIndex < 2) //make sure at least one command is committed
				Thread.Sleep(50);

			WriteLine("<Disconnecting leader!> (" + leader.Name + ")");
			DisconnectNode(leader.Name);

			leader.AppendCommand(commands.Last());

			WriteLine("<Reconnecting leader!> (" + leader.Name + ")");
			ReconnectNode(leader.Name);
			Assert.Equal(RaftEngineState.Leader, leader.State);

			Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.Options.MessageTimeout * nodeCount));

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
			var leader = CreateNetworkAndWaitForLeader(nodeCount);
			var raftNodes = Nodes.ToList();

			var leadersOfNodes = raftNodes.Select(x => x.CurrentLeader).ToList();

			leadersOfNodes.Should().NotContainNulls("After leader is established, all nodes should know that leader exists");
			leadersOfNodes.Should().OnlyContain(l => l.Equals(leader.Name, StringComparison.InvariantCultureIgnoreCase),
				"after leader establishment, all nodes should know only one, selected leader");
		}

		[Fact]
		public void Follower_on_timeout_should_become_candidate()
		{
			var nodeOptions = CreateNodeOptions("realNode", 100, "fakeNode");

			using (var node = new RaftEngine(nodeOptions))
			{
				var timeoutEvent = new ManualResetEventSlim();
				node.StateTimeout += timeoutEvent.Set;

				timeoutEvent.Wait();
				Assert.Equal(RaftEngineState.Candidate, node.State);
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
						var currentConfiguration = persistentState.GetCurrentTopology();
						Assert.Empty(currentConfiguration.AllVotingNodes);

						persistentState.SetCurrentTopology(new Topology(expectedAllVotingPeers), 1);
					}
				}
				using (var options = StorageEnvironmentOptions.ForPath(path))
				{
					using (var persistentState = new PersistentState(options, cancellationTokenSource.Token)
					{
						CommandSerializer = new JsonCommandSerializer()
					})
					{
						var currentConfiguration = persistentState.GetCurrentTopology();
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
			var node = CreateNetworkAndWaitForLeader(3);

			node.State.Should().Be(RaftEngineState.Leader);



		}
	}
}
