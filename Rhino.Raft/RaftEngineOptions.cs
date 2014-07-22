using System;
using System.Collections;
using System.Collections.Generic;
using Rhino.Raft.Interfaces;
using Voron;

namespace Rhino.Raft
{
	public class RaftEngineOptions
	{
		public RaftEngineOptions(string name, StorageEnvironmentOptions options, ITransport transport, IRaftStateMachine stateMachine)
		{
			if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");
			if (options == null) throw new ArgumentNullException("options");
			if (transport == null) throw new ArgumentNullException("transport");
			if (stateMachine == null) throw new ArgumentNullException("stateMachine");

			Name = name;
			Options = options;
			Transport = transport;
			StateMachine = stateMachine;
		}

		public IEnumerable<string> AllPeers { get; set; }

		public string Name { get; private set; }

		public StorageEnvironmentOptions Options { get; private set; }

		public ITransport Transport { get; private set; }

		public IRaftStateMachine StateMachine { get; private set; }
	}
}