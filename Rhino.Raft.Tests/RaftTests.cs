using System;
using System.Collections.Generic;
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
			using (
				var raftNode =
					new RaftEngine(new RaftEngineOptions("node1", StorageEnvironmentOptions.CreateMemoryOnly(), new InMemoryTransport(),
						new DictionaryStateMachine())))
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
						new DictionaryStateMachine())
					{
						AllPeers = new[] { "node2" }
					})
					{
						ElectionTimeout = 1000						
					})
			using (
				var raftNode2 =
					new RaftEngine(new RaftEngineOptions("node2", StorageEnvironmentOptions.CreateMemoryOnly(), transport,
						new DictionaryStateMachine())
					{
						AllPeers = new[] { "node1" }
					})
					{
						ElectionTimeout = 1000000						
					})
			{
				//less election timeout --> will send vote request sooner, and thus expected to become candidate first
				raftNode1.StateChanged += state => candidateChangeEvent.Set();

				Assert.True(candidateChangeEvent.Wait(1000));
				Assert.Equal(RaftEngineState.Candidate, raftNode1.State);
				Assert.Equal(RaftEngineState.Follower, raftNode2.State);
			}
		}
	}
}
