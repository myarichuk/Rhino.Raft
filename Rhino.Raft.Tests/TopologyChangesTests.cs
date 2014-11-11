using System;
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
		public void Adding_additional_node_that_goes_offline_and_then_online_should_still_work()
		{
			var transport = new InMemoryTransport();
			var nodes = CreateRaftNetwork(3, transport).ToList();
			nodes.First().WaitForLeader();

			var leaderNode = nodes.First(x => x.State == RaftEngineState.Leader);

			using (var additionalNode = new RaftEngine(CreateNodeOptions("additional_node", transport, 1500)))
			{
				additionalNode.TopologyChangeStarted += () => transport.DisconnectNode("additional_node");
				var waitForTopologyChangeInLeader =
					leaderNode.WaitForEvent((n, handler) => n.TopologyChangeFinished += cmd => handler(),
					// ReSharper disable once EventUnsubscriptionViaAnonymousDelegate
											(n, handler) => n.TopologyChangeFinished -= cmd => handler());

				leaderNode.AddToClusterAsync(additionalNode.Name).Wait();

				Thread.Sleep(additionalNode.MessageTimeout * 2);
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

			var testDataFolder = Path.Combine(Directory.GetCurrentDirectory(), "test");
			if (Directory.Exists(testDataFolder))
				Directory.Delete(testDataFolder, true);

			var tempFolder = Guid.NewGuid().ToString();
			var nodeApath = Path.Combine(Directory.GetCurrentDirectory(), "test", tempFolder, "A");
			var nodeBpath = Path.Combine(Directory.GetCurrentDirectory(), "test", tempFolder, "B");

			var topologyChangedOnAdditionalNode = new ManualResetEventSlim();

			using (var additionalNode = new RaftEngine(CreateNodeOptions("nodeC", inMemoryTransport, timeout, "nodeC")))
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

				var topologyChangesFinished = new CountdownEvent(2);
				using (var nodeA = new RaftEngine(CreateNodeOptions("nodeA", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeApath), "nodeA", "nodeB")))
				using (var nodeB = new RaftEngine(CreateNodeOptions("nodeB", inMemoryTransport, timeout, StorageEnvironmentOptions.ForPath(nodeBpath), "nodeA", "nodeB")))
				{
					nodeA.TopologyChangeFinished += cmd => topologyChangesFinished.Signal();
					nodeB.TopologyChangeFinished += cmd => topologyChangesFinished.Signal();
					nodeA.WaitForLeader();
					nodeB.WaitForLeader();

					Console.WriteLine("<---nodeA, nodeB are up, waiting for topology change on additional nodes");
					Assert.True(topologyChangesFinished.Wait(15000));
					var condition = topologyChangedOnAdditionalNode.Wait(15000);
					if (condition == false)
					{
						bool a = false;
						while (a == false)
						{
							a = DateTime.Now.Ticks == 1;
							Thread.Sleep(100);
						}
					}
					Assert.True(condition);

					nodeA.AllVotingNodes.Should().Contain(additionalNode.Name);
					nodeB.AllVotingNodes.Should().Contain(additionalNode.Name);
				}
			}
		}

		[Fact]
		public void Cluster_cannot_have_two_concurrent_node_additions()
		{
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(4, messageTimeout: 1500, transport: inMemoryTransport).ToList();
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

		//TODO: test further --> might have a race condition related
		//might happen if additionalNode will have large timeout (much larger than the rest of nodes
		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		public void Node_added_to_cluster_should_update_peers_list(int nodeCount)
		{
			var inMemoryTransport = new InMemoryTransport();
			var raftNodes = CreateRaftNetwork(nodeCount, inMemoryTransport).ToList();
			var topologyChangeTracker = new CountdownEvent(nodeCount + 1);
			raftNodes.First().WaitForLeader();
			using (var additionalNode = new RaftEngine(CreateNodeOptions("nada0", inMemoryTransport, raftNodes.First().MessageTimeout)))
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

			Assert.True(topologyChangeComittedEvent.Wait(nodeCount * 1000));

			var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();
			Trace.WriteLine("<-- expectedNodeNameList:" + expectedNodeNameList.Aggregate(String.Empty, (all, curr) => all + ", " + curr));
			raftNodes.ForEach(node => node.AllVotingNodes.Should().BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
			leader.Dispose();
		}

		[Fact]
		public void Follower_removed_from_cluster_does_not_affect_leader_and_commits()
		{
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(5)
						.All()
						.With(x => x.Completion = null)
						.Build()
						.ToList();

			var raftNodes = CreateRaftNetwork(4, messageTimeout: 1500).ToList();
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
			var raftNodes = CreateRaftNetwork(nodeCount).ToList();
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

			raftNodes.ForEach(node => node.TopologyChangeFinished += cmd => topologyChangeComittedEvent.Signal());

			Trace.WriteLine(string.Format("<-- Removing {0} from the cluster -->", removedNode.Name));
			leader.RemoveFromClusterAsync(removedNode.Name).Wait();

			Assert.True(topologyChangeComittedEvent.Wait(nodeCount * 2500));

			var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();
			Trace.WriteLine("<-- expectedNodeNameList:" + expectedNodeNameList.Aggregate(String.Empty, (all, curr) => all + ", " + curr));
			raftNodes.ForEach(node => node.AllVotingNodes.Should().BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
		}
	}
}
