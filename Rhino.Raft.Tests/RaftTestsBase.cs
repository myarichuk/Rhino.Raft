using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Transport;
using Rhino.Raft.Utils;
using Voron;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class RaftTestsBase : IDisposable
	{
		private readonly List<RaftEngine> _nodes = new List<RaftEngine>();

		private readonly Logger _log = LogManager.GetCurrentClassLogger();
		private readonly InMemoryTransportHub _inMemoryTransportHub;

		protected void ForceTimeout(string name)
		{
			((InMemoryTransportHub.InMemoryTransport)_inMemoryTransportHub.CreateTransportFor(name)).ForceTimeout();
		}

		protected void DisconnectNodeSending(string name)
		{
			_inMemoryTransportHub.DisconnectNodeSending(name);
		}

		protected void DisconnectNode(string name)
		{
			_inMemoryTransportHub.DisconnectNode(name);
		}

		protected void ReconnectNodeSending(string name)
		{
			_inMemoryTransportHub.ReconnectNodeSending(name);
		}

		protected void ReconnectNode(string name)
		{
			_inMemoryTransportHub.ReconnectNode(name);
		}

		public RaftTestsBase()
		{
			_inMemoryTransportHub = new InMemoryTransportHub();
		}

		protected void WriteLine(string format, params object[] args)
		{
			_log.Debug(format, args);
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

		protected ManualResetEventSlim WaitForToplogyChange(RaftEngine node)
		{
			var mre = new ManualResetEventSlim();
			node.TopologyChanged += state => mre.Set();
			return mre;
		}

		protected ManualResetEventSlim WaitForCommit(RaftEngine node, Func<DictionaryStateMachine, bool> predicate)
		{
			var cde = new ManualResetEventSlim();
			node.CommitApplied += command =>
			{
				if (predicate((DictionaryStateMachine)node.StateMachine))
					cde.Set();
			};
			node.SnapshotInstalled += () =>
			{
				var state = (DictionaryStateMachine)node.StateMachine;
				if (predicate(state))
				{
					cde.Set();
				}
			};
			return cde;
		}

		protected ManualResetEventSlim WaitForSnapshot(RaftEngine node)
		{
			var cde = new ManualResetEventSlim();
			node.CreatedSnapshot += cde.Set;
			return cde;
		}

		protected CountdownEvent WaitForCommitsOnCluster(int numberOfCommits)
		{
			var cde = new CountdownEvent(_nodes.Count);
			foreach (var node in _nodes)
			{
				var n = node;
				if (n.CommitIndex == numberOfCommits && cde.CurrentCount > 0)
				{
					cde.Signal();
					continue;
				}
				n.CommitApplied += command =>
				{
					if (n.CommitIndex == numberOfCommits && cde.CurrentCount > 0)
						cde.Signal();
				};
				n.SnapshotInstalled += () =>
				{
					if (n.CommitIndex == numberOfCommits && cde.CurrentCount > 0)
						cde.Signal();
				};
			}

			return cde;
		}

		protected CountdownEvent WaitForCommitsOnCluster(Func<DictionaryStateMachine, bool> predicate)
		{
			var cde = new CountdownEvent(_nodes.Count);
			var votedAlready = new ConcurrentDictionary<RaftEngine, object>();
			
			foreach (var node in _nodes)
			{
				var n = node;
				n.CommitApplied += command =>
				{
					var state = (DictionaryStateMachine)n.StateMachine;
					if (predicate(state) && cde.CurrentCount > 0)
					{
						if (votedAlready.ContainsKey(n))
							return;
						votedAlready.TryAdd(n, n);
						_log.Debug("WaitForCommitsOnCluster match " + n.Name + " " + state.Data.Count);
						cde.Signal();
					}
				};
				n.SnapshotInstalled += () =>
				{
					var state = (DictionaryStateMachine) n.StateMachine;
					if (predicate(state) && cde.CurrentCount > 0)
					{
						if (votedAlready.ContainsKey(n))
							return;
						votedAlready.TryAdd(n, n);

						_log.Debug("WaitForCommitsOnCluster match"); 
						cde.Signal();
					}
				};
			}
			
			return cde;
		}


		protected CountdownEvent WaitForToplogyChangeOnCluster(List<RaftEngine> raftNodes = null)
		{
			raftNodes = raftNodes ?? _nodes;
			var cde = new CountdownEvent(raftNodes.Count);
			foreach (var node in raftNodes)
			{
				var n = node;
				n.TopologyChanged += (a) =>
				{
					if (cde.CurrentCount > 0)
					{
						cde.Signal();
					}
				};
			}

			return cde;
		}
		protected ManualResetEventSlim WaitForSnapshotInstallation(RaftEngine node)
		{
			var cde = new ManualResetEventSlim();
			node.SnapshotInstalled += cde.Set;
			return cde;
		}

		protected Task<RaftEngine> WaitForNewLeaderAsync()
		{
			var rcs = new TaskCompletionSource<RaftEngine>();
			foreach (var node in _nodes)
			{
				var n = node;

				n.ElectedAsLeader += () => rcs.TrySetResult(n);
			}

			return rcs.Task;
		}

		protected RaftEngine CreateNetworkAndWaitForLeader(int nodeCount, int messageTimeout = -1)
		{
			var raftNodes = CreateNodeNetwork(nodeCount, messageTimeout: messageTimeout).ToList();
			var raftEngine = _nodes[new Random().Next(0, _nodes.Count)];

			var nopCommit = WaitForCommitsOnCluster(1); // nop commit

			var transport = (InMemoryTransportHub.InMemoryTransport)_inMemoryTransportHub.CreateTransportFor(raftEngine.Name);
			transport.ForceTimeout();

			raftNodes.First().WaitForLeader();

			nopCommit.Wait();

			var leader = raftNodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);
			return leader;
		}


		protected RaftEngineOptions CreateNodeOptions(string nodeName, int messageTimeout, params string[] peers)
		{
			var nodeOptions = new RaftEngineOptions(nodeName,
				StorageEnvironmentOptions.CreateMemoryOnly(),
				_inMemoryTransportHub.CreateTransportFor(nodeName),
				new DictionaryStateMachine())
			{
				MessageTimeout = messageTimeout,
				AllVotingNodes = peers,
				Stopwatch = Stopwatch.StartNew()
			};
			return nodeOptions;
		}

		protected RaftEngineOptions CreateNodeOptions(string nodeName,int messageTimeout, StorageEnvironmentOptions storageOptions, params string[] peers)
		{
			var nodeOptions = new RaftEngineOptions(nodeName,
				storageOptions,
				_inMemoryTransportHub.CreateTransportFor(nodeName),
				new DictionaryStateMachine())
			{
				MessageTimeout = messageTimeout,
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
			var raftEngine = new RaftEngine(CreateNodeOptions("node" + _nodes.Count, leader.Options.MessageTimeout));
			_nodes.Add(raftEngine);
			return raftEngine;
		}

		protected IEnumerable<RaftEngine> CreateNodeNetwork(int nodeCount, ITransport transport = null, int messageTimeout = -1, Func<RaftEngineOptions,RaftEngineOptions> optionChangerFunc = null)
		{
			if (messageTimeout == -1)
				messageTimeout = Debugger.IsAttached ? 60*1000 : 1000;
			var nodeNames = new List<string>();
			for (int i = 0; i < nodeCount; i++)
			{
				nodeNames.Add("node" + i);
			}

			if (optionChangerFunc == null)
				optionChangerFunc = options => options;

			var raftNetwork = nodeNames
				.Select(name => optionChangerFunc(CreateNodeOptions(name, messageTimeout, nodeNames.ToArray())))
				.Select(nodeOptions => new RaftEngine(nodeOptions))
				.ToList();

			_nodes.AddRange(raftNetwork);

			return raftNetwork;
		}

		public virtual void Dispose()
		{
			_nodes.ForEach(node => node.Dispose());
		}
	}
}