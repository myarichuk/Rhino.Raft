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
		private readonly CancellationTokenSource _cancellationTokenSource;

		public RaftTests()
		{
			_cancellationTokenSource = new CancellationTokenSource();
		}

		[Fact]
		public void AfterHeartbeatTimeout_Node_should_change_state_to_candidate()
		{
			using(var transport = new InMemoryTransport("node1"))
			{
				var raftNode1 = new RaftEngine("node1",
					StorageEnvironmentOptions.CreateMemoryOnly(),
					transport,
					new DictionaryStateMachine(), _cancellationTokenSource.Token)
				{
					HeartbeatTimeout = TimeSpan.FromSeconds(1)
				};

				Thread.Sleep(1005);

				Assert.Equal(RaftEngineState.Candidate, raftNode1.State);
			}
		}
	}
}
