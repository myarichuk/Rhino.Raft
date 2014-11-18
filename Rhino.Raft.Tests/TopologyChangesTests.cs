using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Voron;
using Xunit;
using Xunit.Extensions;

namespace Rhino.Raft.Tests
{
	public class TopologyChangesTests : RaftTestsBase
	{
		[Fact]
		public void CanRevertTopologyChange()
		{
			var leader = CreateNetworkAndWaitForLeader(3);
			var nonLeaders = Nodes.Where(x => x != leader).ToList();
			var inMemoryTransport = ((InMemoryTransport) leader.Transport);
			var topologyChnaged = WaitForToplogyChange(leader);
			inMemoryTransport.DisconnectNodeSending(leader.Name);

			WriteLine("Initial leader is " + leader.Name);
			leader.AddToClusterAsync("node3");

			Assert.True(leader.ContainedInAllVotingNodes("node2"));

			WriteLine("<-- should switch leaders now");

			nonLeaders.ForEach(engine => inMemoryTransport.ForceTimeout(engine.Name));

			inMemoryTransport.ForceTimeout(nonLeaders[0].Name);// force it to win

			topologyChnaged.Wait();

			Assert.True(nonLeaders.Any(x=>x.State==RaftEngineState.Leader));
			foreach (var raftEngine in nonLeaders)
			{
				Assert.False(raftEngine.ContainedInAllVotingNodes("node3"));
			}
			Assert.False(leader.ContainedInAllVotingNodes("node3"));

		}

		[Fact]
		public void New_node_can_be_added_even_if_it_is_down()
		{
			const int nodeCount = 3;

			var topologyChangeFinishedOnAllNodes = new CountdownEvent(nodeCount);
			var nodes = CreateNodeNetwork(nodeCount).ToList();
			nodes.First().WaitForLeader();
			var leader = nodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			leader.Should().NotBeNull("if at this stage no leader was selected then something is really wrong");

			nodes.ForEach(n => n.TopologyChanged += cmd => topologyChangeFinishedOnAllNodes.Signal());

			// ReSharper disable once PossibleNullReferenceException
			leader.AddToClusterAsync("non-existing-node").Wait();

			Assert.True(topologyChangeFinishedOnAllNodes.Wait(5000),"Topology changes should happen in less than 5 sec for 3 node network");
			nodes.ForEach(n => n.CurrentTopology.AllVotingNodes.Should().Contain("non-existing-node"));
		}


		[Fact]
		public void Adding_additional_node_that_goes_offline_and_then_online_should_still_work()
		{
			var transport = new InMemoryTransport();
			var nodes = CreateNodeNetwork(3, transport).ToList();
			nodes.First().WaitForLeader();

			var leaderNode = nodes.First(x => x.State == RaftEngineState.Leader);

			using (var additionalNode = new RaftEngine(CreateNodeOptions("additional_node", transport, 1500)))
			{
				additionalNode.TopologyChanging += () => transport.DisconnectNode("additional_node");
				var waitForTopologyChangeInLeader =
					leaderNode.WaitForEventTask((n, handler) => n.TopologyChanged += cmd => handler());

				leaderNode.AddToClusterAsync(additionalNode.Name).Wait();

				Thread.Sleep(additionalNode.Options.MessageTimeout * 2);
				transport.ReconnectNode(additionalNode.Name);

				Assert.True(waitForTopologyChangeInLeader.Wait(3000));
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
		public void Removal_of_non_existing_node_should_throw()
		{
			using (var node = new RaftEngine(CreateNodeOptions("node", new InMemoryTransport(), 1500, "node")))
			{
				node.Invoking(x => x.RemoveFromClusterAsync("non-existing"))
					.ShouldThrow<InvalidOperationException>();
			}

		}

		[Fact]
		public void Cluster_cannot_have_two_concurrent_node_removals()
		{
			var raftNodes = CreateNodeNetwork(4, messageTimeout: 1500).ToList();
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
		public void Node_added_to_cluster_will_not_cause_new_election(int nodeCount)
		{
			var electionStartedEvent = new ManualResetEventSlim();
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateNodeNetwork(nodeCount, inMemoryTransport).ToList();

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
		[InlineData(5)]
		[InlineData(7)]
		public void Non_leader_Node_removed_from_cluster_should_update_peers_list(int nodeCount)
		{
			var leader = CreateNetworkAndWaitForLeader(nodeCount);

			var nodeToRemove = Nodes.First(x => x.State != RaftEngineState.Leader);

			var nodesThatShouldRemain = Nodes.Where(n => ReferenceEquals(n, nodeToRemove) == false)
												 .Select(n => n.Name)
												 .ToList();

			var list = (
				from engine in Nodes
				where engine != nodeToRemove 
				select WaitForToplogyChange(engine)
			).ToList();

			Trace.WriteLine("<--- started removing");
			leader.RemoveFromClusterAsync(nodeToRemove.Name).Wait();

			foreach (var slim in list)
			{
				slim.Wait();
			}

			var nodePeerLists = Nodes.Where(n => ReferenceEquals(n, nodeToRemove) == false)
										 .Select(n => n.AllVotingNodes)
										 .ToList();

			nodePeerLists.ForEach(peerList => peerList.ShouldBeEquivalentTo(nodesThatShouldRemain));
		}


		[Fact]
		public void Cluster_nodes_are_able_to_recover_after_shutdown_in_the_middle_of_topology_change()
		{
			const int timeout = 2500;
			var inMemoryTransport = new InMemoryTransport();

			var testDataFolder = Path.Combine(Directory.GetCurrentDirectory(), "test");
			if (Directory.Exists(testDataFolder))
				Directory.Delete(testDataFolder, true);

			var tempFolder = Guid.NewGuid().ToString();
			var nodeApath = Path.Combine(Directory.GetCurrentDirectory(), "test", tempFolder, "A");
			var nodeBpath = Path.Combine(Directory.GetCurrentDirectory(), "test", tempFolder, "B");

			inMemoryTransport.DisconnectNode("nodeC");
			var topologyChangeStarted = new ManualResetEventSlim();
			string nonLeaderName;
			using (
				var nodeA =
					new RaftEngine(CreateNodeOptions("nodeA", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeApath),
						"nodeA", "nodeB")))
			using (
				var nodeB =
					new RaftEngine(CreateNodeOptions("nodeB", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeBpath),
						"nodeA", "nodeB")))
			{
				inMemoryTransport.ForceTimeout("nodeA");
				nodeA.WaitForLeader();
				Assert.True(nodeA.State != nodeB.State,
					String.Format("nodeA.State {0} != nodeB.State {1}", nodeA.State, nodeB.State));


				var leader = nodeA.State == RaftEngineState.Leader ? nodeA : nodeB;
				var nonLeader = nodeA.State != RaftEngineState.Leader ? nodeA : nodeB;
				nonLeaderName = nonLeader.Name;

				nonLeader.TopologyChanging += () =>
				{
					Console.WriteLine("<---disconnected from sending : " + nonLeaderName);
					inMemoryTransport.DisconnectNodeSending(nonLeaderName);
					topologyChangeStarted.Set();
				};

				Console.WriteLine("<---adding new node here");
				leader.AddToClusterAsync("nodeC");
				Assert.True(topologyChangeStarted.Wait(2000));
			}
			WriteLine("<---nodeA, nodeB are down");

			inMemoryTransport.ReconnectNodeSending(nonLeaderName);

			var topologyChangesFinished = new CountdownEvent(2);
			using (
				var nodeA =
					new RaftEngine(CreateNodeOptions("nodeA", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeApath),
						"nodeA", "nodeB")))
			using (
				var nodeB =
					new RaftEngine(CreateNodeOptions("nodeB", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeBpath),
						"nodeA", "nodeB")))
			{
				nodeA.TopologyChanged += cmd => topologyChangesFinished.Signal();
				nodeB.TopologyChanged += cmd => topologyChangesFinished.Signal();

				inMemoryTransport.ForceTimeout("nodeA");

				nodeA.WaitForLeader();
				nodeB.WaitForLeader();

				WriteLine("<---nodeA, nodeB are up, waiting for topology change on additional nodes");
				Assert.True(topologyChangesFinished.Wait(3000));

				nodeA.AllVotingNodes.Should().Contain("nodeC");
				nodeB.AllVotingNodes.Should().Contain("nodeC");
			}
		}

		[Fact]
		public void Cluster_cannot_have_two_concurrent_node_additions()
		{
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateNodeNetwork(4, messageTimeout: 1500, transport: inMemoryTransport).ToList();
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

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		public void Node_added_to_cluster_should_update_peers_list(int nodeCount)
		{
			var leader = CreateNetworkAndWaitForLeader(nodeCount);
			using (var additionalNode = new RaftEngine(CreateNodeOptions("nada0", leader.Transport, leader.Options.MessageTimeout)))
			{
				var clusterChanged = WaitForToplogyChangeOnCluster();
				var newNodeAdded = WaitForToplogyChange(additionalNode);

				leader.AddToClusterAsync(additionalNode.Name).Wait();

				clusterChanged.Wait();
				newNodeAdded.Wait();

				var raftNodes = Nodes.ToList();
				foreach (var node in raftNodes)
				{
					var containedInAllVotingNodes = node.ContainedInAllVotingNodes(additionalNode.Name);
					if(containedInAllVotingNodes)
						continue;
					Assert.True(containedInAllVotingNodes,
						string.Join(", ", node.AllVotingNodes));
				}

				additionalNode.AllVotingNodes.Should().Contain(raftNodes.Select(node => node.Name));
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		public void Can_step_down(int nodeCount)
		{
			var leader = CreateNetworkAndWaitForLeader(nodeCount);

			var firstCommits = WaitForCommitsOnCluster(x => x.Data.ContainsKey("4"));
			for (int i = 0; i < 5; i++)
			{
				leader.AppendCommand(new DictionaryCommand.Set
				{
					Key = i.ToString(),
					Value = i
				});
			}
			firstCommits.Wait();

			var nextCommit = WaitForCommitsOnCluster(x => x.Data.ContainsKey("c"));

			leader.AppendCommand(new DictionaryCommand.Set
			{
				Key = "c",
				Value = 3
			});

			var newLeader = WaitForNewLeaderAsync();

			leader.StepDownAsync().Wait();

			nextCommit.Wait();

			var dictionaryStateMachine = ((DictionaryStateMachine)newLeader.Result.StateMachine);
			WriteLine("<-- have new leader state machine");

			Assert.Equal(3, dictionaryStateMachine.Data["c"]);
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		public void Leader_removed_from_cluster_modifies_member_lists_on_remaining_nodes(int nodeCount)
		{

			var leader = CreateNetworkAndWaitForLeader(nodeCount);
			var raftNodes = Nodes.ToList();
			var nonLeaderNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader);
			Assert.NotNull(leader);
			Assert.NotNull(nonLeaderNode);

			raftNodes.Remove(leader);
			var waitForNewLeaderAsync = WaitForNewLeaderAsync();

			leader.StepDownAsync().Wait();

			waitForNewLeaderAsync.Result.RemoveFromClusterAsync(leader.Name).Wait();

			var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();

			raftNodes.ForEach(node => node.AllVotingNodes.Should()
				.BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
		}

		[Fact]
		public void Follower_removed_from_cluster_does_not_affect_leader_and_commits()
		{
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(5)
						.All()
						.With(x => x.Completion = null)
						.Build()
						.ToList();

			var raftNodes = CreateNodeNetwork(4, messageTimeout: 1500).ToList();
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

			otherNonLeaderNode.CommitIndex.Should().Be(leader.CommitIndex, "after all commands have been comitted, on non-leader nodes should be the same commit index as on index node");
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		public void Follower_removed_from_cluster_modifies_member_lists_on_remaining_nodes(int nodeCount)
		{
			var raftNodes = CreateNodeNetwork(nodeCount).ToList();
			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(n => n.State == RaftEngineState.Leader);
			var removedNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader);
			var nonLeaderNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader && !ReferenceEquals(n, removedNode));
			Assert.NotNull(leader);
			Assert.NotNull(removedNode);

			Trace.WriteLine(string.Format("<-- Leader chosen: {0} -->", leader.Name));
			Trace.WriteLine(string.Format("<-- Node to be removed: {0} -->", removedNode.Name));

			raftNodes.Remove(removedNode);
			var topologyChangeComittedEvent = new CountdownEvent(nodeCount - 1);

			raftNodes.ForEach(node => node.TopologyChanged += cmd => topologyChangeComittedEvent.Signal());

			Trace.WriteLine(string.Format("<-- Removing {0} from the cluster -->", removedNode.Name));
			leader.RemoveFromClusterAsync(removedNode.Name).Wait();

			Assert.True(topologyChangeComittedEvent.Wait(nodeCount * 2500));

			var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();
			Trace.WriteLine("<-- expectedNodeNameList:" + expectedNodeNameList.Aggregate(String.Empty, (all, curr) => all + ", " + curr));
			raftNodes.ForEach(node => node.AllVotingNodes.Should().BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
		}
	}
}
