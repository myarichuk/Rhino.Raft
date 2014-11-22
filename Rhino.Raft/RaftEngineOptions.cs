using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Transport;
using Voron;
using Voron.Impl;

namespace Rhino.Raft
{
	public class RaftEngineOptions
	{
		public RaftEngineOptions(string name, StorageEnvironmentOptions storageOptions, ITransport transport, IRaftStateMachine stateMachine)
		{
			if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");
			if (storageOptions == null) throw new ArgumentNullException("storageOptions");
			if (transport == null) throw new ArgumentNullException("transport");
			if (stateMachine == null) throw new ArgumentNullException("stateMachine");

			SelfConnection = new NodeConnectionInfo
			{
				Name = name
			};
			StorageOptions = storageOptions;
			Transport = transport;
			StateMachine = stateMachine;
			MessageTimeout = 300;
			Stopwatch = new Stopwatch();
			MaxLogLengthBeforeCompaction = 32 * 1024;
			MaxStepDownDrainTime = TimeSpan.FromSeconds(15);
			MaxEntriesPerRequest = 256;
		}

		public int MaxEntriesPerRequest { get; set; }
		public TimeSpan MaxStepDownDrainTime { get; set; }

		public int MaxLogLengthBeforeCompaction { get; set; }

		public Stopwatch Stopwatch { get; set; }

		public string Name { get { return SelfConnection.Name; } }

		public StorageEnvironmentOptions StorageOptions { get; private set; }

		public ITransport Transport { get; private set; }

		public IRaftStateMachine StateMachine { get; private set; }

		public int MessageTimeout { get; set; }

		public NodeConnectionInfo SelfConnection { get; set; }
	}
}