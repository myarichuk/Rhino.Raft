using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Voron;
using Xunit;
using Xunit.Extensions;

namespace Rhino.Raft.Tests
{
	public class RaftTests
	{
		[Fact]
		public void Single_nod_is_a_leader_automatically()
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
		public void AfterHeartbeatTimeout_Node_should_change_state_to_candidate()
		{
			var candidateChangeEvent = new ManualResetEventSlim();
			var transport = new InMemoryTransport();
			var node1Options = CreateNodeOptions("node1", transport, 1000, "node2");
			var node2Options = CreateNodeOptions("node2", transport, 10000, "node1");

			using (var raftNode1 = new RaftEngine(node1Options))
			using (new RaftEngine(node2Options))
			{
				//less election timeout --> will send vote request sooner, and thus expected to become candidate first
				raftNode1.StateChanged += state => candidateChangeEvent.Set();

				candidateChangeEvent.Wait();
				Assert.Equal(RaftEngineState.Candidate, raftNode1.State);
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
				raftNodes = CreateRaftNetwork(nodeCount,messageTimeout:100).ToList();
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

		[Fact]
		public void On_many_node_network_after_leader_establishment_all_nodes_know_who_is_leader()
		{
			var leaderEvent = new ManualResetEventSlim();
			var followerEvent = new CountdownEvent(4);

			List<RaftEngine> raftNodes = null;
			try
			{
				raftNodes = CreateRaftNetwork(5).ToList();

				raftNodes.ForEach(node => node.StateChanged += state =>
				{
					if (state == RaftEngineState.Leader)
						leaderEvent.Set();
					else if (state == RaftEngineState.Follower)
						followerEvent.Signal();

				});

				Assert.True(leaderEvent.Wait(25000));

				var leaderNode = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
				Assert.NotNull(leaderNode);

				followerEvent.Wait(15000); //wait until all other nodes become followers

				Assert.True(raftNodes.Select(x => x.CurrentLeader).All(x => x != null && x.Equals(leaderNode.CurrentLeader)));
				
			}
			finally
			{
				if (raftNodes != null) raftNodes.ForEach(node => node.Dispose());
			}
			
		}

		[Fact]
		public void On_many_node_network_after_leader_establishment_commands_are_distritbuted_to_follower()
		{
			var leaderEvent = new ManualResetEventSlim();
			var followerEvent = new CountdownEvent(3);

			List<RaftEngine> raftNodes = null;
			try
			{
				raftNodes = CreateRaftNetwork(4).ToList();

				raftNodes.ForEach(node => node.StateChanged += state =>
				{
					if (state == RaftEngineState.Leader)
						leaderEvent.Set();
					else if (state == RaftEngineState.Follower && followerEvent.CurrentCount > 0)
						followerEvent.Signal();

				});

				Assert.True(leaderEvent.Wait(2000));
				var leaderNode = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);				
				Assert.True(followerEvent.Wait(8000)); //wait until all other nodes become followers

				var nonLeaderNode = raftNodes.FirstOrDefault(x => x != leaderNode);

				// ReSharper disable once PossibleNullReferenceException
				//if you try to append command on non-leader node,exception should be thrown
//				Assert.Throws<InvalidOperationException>(() => nonLeaderNode.AppendCommand(new NopCommand()));

// ReSharper disable once PossibleNullReferenceException
				leaderNode.AppendCommand(new DictionaryCommand.Set
				{
					Key = "Foo",
					Value = 123
				});

				leaderNode.AppendCommand(new DictionaryCommand.Set
				{
					Key = "Bar",
					Value = 456
				});

				leaderNode.AppendCommand(new DictionaryCommand.Del
				{
					Key = "Foo"
				});

				leaderNode.AppendCommand(new DictionaryCommand.Inc
				{
					Key = "Bar",
					Value = 5
				});

				
			}
			finally
			{
				if (raftNodes != null) raftNodes.ForEach(node => node.Dispose());
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
				AllPeers = peers
			};
			return node1Options;
		}

		private IEnumerable<RaftEngine> CreateRaftNetwork(int nodeCount, ITransport transport = null, int messageTimeout = 1000,Func<RaftEngineOptions,RaftEngineOptions> optionChangerFunc = null)
		{
			transport = transport ?? new InMemoryTransport();
			var nodeNames = new List<string>();
			for (int i = 0; i < nodeCount; i++)
			{
				nodeNames.Add("node" + i);
			}

			if(optionChangerFunc == null)
				return nodeNames.Select(name => CreateNodeOptions(name, transport, messageTimeout, nodeNames.Where(x => !x.Equals(name)).ToArray()))
								.Select(nodeOptions => new RaftEngine(nodeOptions))
								.ToList();

			return nodeNames.Select(name => optionChangerFunc(CreateNodeOptions(name, transport, messageTimeout, nodeNames.Where(x => !x.Equals(name)).ToArray())))
				.Select(nodeOptions => new RaftEngine(nodeOptions))
				.ToList();
		}
	}
}
