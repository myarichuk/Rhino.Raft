using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Utils;
using Voron;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class RaftTestsBase : IDisposable
	{
		private readonly List<RaftEngine> _nodes = new List<RaftEngine>();

		private readonly DebugWriter _writer = new DebugWriter("Test", Stopwatch.StartNew());

		protected void WriteLine(string format, params object[] args)
		{
			_writer.Write(format, args);
		}

		public IEnumerable<RaftEngine> Nodes { get { return _nodes; } }

		protected ManualResetEventSlim WaitForStateChange(RaftEngine node, RaftEngineState requestedState)
		{
			var mre = new ManualResetEventSlim();
			node.StateChanged += state =>
			{
				if (state == requestedState)
					mre.Set();
			};
			return mre;
		}

		protected ManualResetEventSlim WaitForCommit<T>(RaftEngine node, Func<DictionaryStateMachine, bool> predicate)
		{
			var cde = new ManualResetEventSlim();
			node.CommitApplied += command =>
			{
				if (predicate((DictionaryStateMachine)node.StateMachine))
					cde.Set();
			};
			return cde;
		}

		protected ManualResetEventSlim WaitForSnapshot(RaftEngine node)
		{
			var cde = new ManualResetEventSlim();
			node.SnapshotCreationEnded += cde.Set;
			return cde;
		}

		protected CountdownEvent WaitForCommitsOnCluster(Func<DictionaryStateMachine, bool> predicate)
		{
			var cde = new CountdownEvent(_nodes.Count);
			foreach (var node in _nodes)
			{
				var n = node;
				n.CommitApplied += command =>
				{
					var state = (DictionaryStateMachine)n.StateMachine;
					if (predicate(state) && cde.CurrentCount > 0)
					{
						n.DebugLog.Write("WaitForCommitsOnCluster match");
						cde.Signal();
					}
				};
				n.SnapshotInstallationEnded += () =>
				{
					var state = (DictionaryStateMachine) n.StateMachine;
					if (predicate(state) && cde.CurrentCount > 0)
					{
						n.DebugLog.Write("WaitForCommitsOnCluster match"); 
						cde.Signal();
					}
				};
			}
			
			return cde;
		}
		protected ManualResetEventSlim WaitForSnapshotInstallation(RaftEngine node)
		{
			var cde = new ManualResetEventSlim();
			node.SnapshotInstallationEnded += cde.Set;
			return cde;
		}

		protected RaftEngine CreateNetworkAndWaitForLeader(int nodeCount, int messageTimeout = -1)
		{
			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: messageTimeout).ToList();
			var raftEngine = _nodes[new Random().Next(0, _nodes.Count)];

			((InMemoryTransport) raftEngine.Transport).ForceTimeout(raftEngine.Name);

			raftNodes.First().WaitForLeader();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);
			return leader;
		}

		protected RaftEngine CreateNodeWithVirtualNetworkAndMakeItLeader(string leaderNodeName, ITransport transport, params string[] virtualPeers)
		{
			var leaderNode = new RaftEngine(CreateNodeOptions(leaderNodeName, transport, 1500, virtualPeers));
			leaderNode.WaitForEventTask(
				(node, handler) => node.ElectionStarted += handler).Wait();

			foreach(var peer in virtualPeers)
				transport.Send(leaderNodeName,new RequestVoteResponse
				{
					From = peer,
					Term = 1,
					VoteGranted = true
				});

			leaderNode.WaitForLeader();

			_nodes.Add(leaderNode);
			return leaderNode;
		}

		protected RaftEngine CreateNodeWithVirtualNetwork(string nodeName, ITransport transport, params string[] virtualPeers)
		{
			var node = new RaftEngine(CreateNodeOptions(nodeName, transport, 1500, virtualPeers));

			var waitForEventLoop = node.WaitForEventTask(
				(n, handler) => n.EventsProcessed += handler);

			transport.Send(nodeName, new AppendEntriesRequest
			{
				Entries = new LogEntry[0],
				From = virtualPeers.First(),
				LeaderCommit = 1,
				LeaderId = virtualPeers.First(),
				Term = 1,
				PrevLogIndex = 0,
				PrevLogTerm = 0
			});

			waitForEventLoop.Wait();
			_nodes.Add(node);
			return node;
		}


		protected static RaftEngineOptions CreateNodeOptions(string nodeName, ITransport transport, int messageTimeout, params string[] peers)
		{
			var nodeOptions = new RaftEngineOptions(nodeName,
				StorageEnvironmentOptions.CreateMemoryOnly(),
				transport,
				new DictionaryStateMachine(), 
				messageTimeout)
			{
				AllVotingNodes = peers,
				Stopwatch = Stopwatch.StartNew()
			};
			return nodeOptions;
		}

		protected static RaftEngineOptions CreateNodeOptions(string nodeName, ITransport transport, int messageTimeout, StorageEnvironmentOptions storageOptions, params string[] peers)
		{
			var nodeOptions = new RaftEngineOptions(nodeName,
				storageOptions,
				transport,
				new DictionaryStateMachine(),
				messageTimeout)
			{
				AllVotingNodes = peers,
				Stopwatch = Stopwatch.StartNew()
			};
			return nodeOptions;
		}

		protected bool AreEqual(byte[] array1, byte[] array2)
		{
			if (array1.Length != array2.Length)
				return false;

			return !array1.Where((t, i) => t != array2[i]).Any();
		}


		protected RaftEngine NewNodeFor(RaftEngine leader)
		{
			var raftEngine = new RaftEngine(CreateNodeOptions("node" + _nodes.Count, leader.Transport, leader.MessageTimeout));
			_nodes.Add(raftEngine);
			return raftEngine;
		}

		protected IEnumerable<RaftEngine> CreateNodeNetwork(int nodeCount, ITransport transport = null, int messageTimeout = -1, Func<RaftEngineOptions,RaftEngineOptions> optionChangerFunc = null)
		{
			if (messageTimeout == -1)
				messageTimeout = Debugger.IsAttached ? 60*1000 : 1000;
			transport = transport ?? new InMemoryTransport();
			var nodeNames = new List<string>();
			for (int i = 0; i < nodeCount; i++)
			{
				nodeNames.Add("node" + i);
			}

			if (optionChangerFunc == null)
				optionChangerFunc = options => options;

			var raftNetwork = nodeNames
				.Select(name => optionChangerFunc(CreateNodeOptions(name, transport, messageTimeout, nodeNames.ToArray())))
				.Select(nodeOptions => new RaftEngine(nodeOptions))
				.ToList();

			_nodes.AddRange(raftNetwork);

			return raftNetwork;
		}

		public void Dispose()
		{
			_nodes.ForEach(node => node.Dispose());
		}
	}
}