using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Rhino.Raft.Interfaces;
using Voron;
using Voron.Impl;

namespace Rhino.Raft
{
	public class RaftEngineOptions
	{
		public RaftEngineOptions(string name, StorageEnvironmentOptions options, ITransport transport, IRaftStateMachine stateMachine, int messageTimeout, long commandCommitTimeout = 60000)
		{
			if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");
			if (options == null) throw new ArgumentNullException("options");
			if (transport == null) throw new ArgumentNullException("transport");
			if (stateMachine == null) throw new ArgumentNullException("stateMachine");

			Name = name;
			Options = options;
			Transport = transport;
			StateMachine = stateMachine;
			MessageTimeout = messageTimeout;
			Stopwatch = new Stopwatch();
			CommandCommitTimeout = commandCommitTimeout;
			MaxLogLengthBeforeCompaction = 32 * 1024;
		}

		public int MaxLogLengthBeforeCompaction { get; set; }

		public bool ForceNewTopology { get; set; }

		public Stopwatch Stopwatch { get; set; }

		public IEnumerable<string> AllVotingNodes { get; set; }

		public string Name { get; private set; }

		public StorageEnvironmentOptions Options { get; private set; }

		public ITransport Transport { get; private set; }

		public IRaftStateMachine StateMachine { get; private set; }

		public int MessageTimeout { get; set; }

		public long CommandCommitTimeout { get; set; }
	}
}