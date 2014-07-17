using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Rhino.Raft
{
	public class RaftEngine : IDisposable
	{
		private readonly Task _eventLoopThread;
		private readonly Task _heartbeatThread;
		private readonly RaftEngineState _state;

		private bool _isEventLoopRunning;

		public RaftEngine()
		{
			_state = RaftEngineState.Follower; // each Raft node starts in follower state
			_isEventLoopRunning = true;
		}

		public RaftEngineState State
		{
			get { return _state; }
		}

		public void Dispose()
		{
			_isEventLoopRunning = false;
		}
	}
}
