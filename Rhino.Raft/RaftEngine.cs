using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Implementations.StateBehaviors;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft
{
	public class RaftEngine : IDisposable
	{
		private readonly Task _eventLoopThread;
		private readonly RaftEngineState _state;
		private readonly CancellationToken _cancellationToken;
		
		private readonly ITransport _transport;
		private readonly ICommandSerializer _commandCommandSerializer;
		private IStateBehavior _currentStateBehavior;
		private bool _isEventLoopRunning;

		private readonly string _name;

		public TimeSpan HeartbeatTimeout { get; set; }
		public TimeSpan ElectionTimeout { get; set; }
		public string CurrentLeader { get; private set; }

		public RaftEngine(ITransport transport, ICommandSerializer commandCommandSerializer, string name, CancellationToken cancellationToken)
		{
			if (transport == null) throw new ArgumentNullException("transport");
			if (commandCommandSerializer == null) throw new ArgumentNullException("commandCommandSerializer");
			if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

			CurrentLeader = String.Empty;

			_transport = transport;
			
			_cancellationToken = cancellationToken;
			_name = name;
			_commandCommandSerializer = commandCommandSerializer;
			_currentStateBehavior = new FollowerStateBehavior(_transport,CurrentLeader);

			_state = RaftEngineState.Follower; // each Raft node starts in follower state
			_isEventLoopRunning = true;
		}



		public RaftEngineState State
		{
			get { return _state; }
		}

		public string Name
		{
			get { return _name; }
		}

		public void BehaviorEventLoop()
		{
			while (_isEventLoopRunning)
			{
				_cancellationToken.ThrowIfCancellationRequested();
				if (_currentStateBehavior.ShouldChangeState)
					_currentStateBehavior = _currentStateBehavior.GetNextState();

				_currentStateBehavior.DispatchEvents();				
			}
		}

		public void Dispose()
		{
			_isEventLoopRunning = false;
		}
	}
}
