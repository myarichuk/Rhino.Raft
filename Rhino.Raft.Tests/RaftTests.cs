using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Voron;
using Xunit;

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
			using (
				var raftNode1 =
					new RaftEngine(new RaftEngineOptions("node1", StorageEnvironmentOptions.CreateMemoryOnly(), transport,
						new DictionaryStateMachine(),1000)
					{
						AllPeers = new[] { "node2" }
					}))
			using (new RaftEngine(new RaftEngineOptions("node2", StorageEnvironmentOptions.CreateMemoryOnly(), transport,
				new DictionaryStateMachine(),10000)
			{
				AllPeers = new[] { "node1" }
			}))
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
			using (
				var raftNode1 =
					new RaftEngine(new RaftEngineOptions("node1", StorageEnvironmentOptions.CreateMemoryOnly(), transport,
						new DictionaryStateMachine(),1000)
					{
						AllPeers = new[] { "node2" }
					}))
			using (var raftNode2 = new RaftEngine(new RaftEngineOptions("node2", StorageEnvironmentOptions.CreateMemoryOnly(), transport,
				new DictionaryStateMachine(),10000)
			{
				AllPeers = new[] { "node1" }
			}))
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
	}
}
