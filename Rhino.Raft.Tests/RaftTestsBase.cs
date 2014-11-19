using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;
using Rhino.Raft.Transport;
using Rhino.Raft.Utils;
using Voron;
using Voron.Util;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class RaftTestsBase : IDisposable
	{
		private readonly List<RaftEngine> _nodes = new List<RaftEngine>();

		private readonly Logger _log = LogManager.GetCurrentClassLogger();
		protected readonly InMemoryTransportHub _inMemoryTransportHub;

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

		protected void RestartAllNodes()
		{
			foreach (var raftEngine in _nodes)
			{
				raftEngine.Options.StorageOptions.OwnsPagers = false;
				raftEngine.Dispose();
			}
			for (int i = 0; i < _nodes.Count; i++)
			{
				_nodes[i] = new RaftEngine(_nodes[i].Options);
			}
		}

		protected RaftEngine CreateNetworkAndGetLeader(int nodeCount, int messageTimeout = -1, bool waitForLeader = true)
		{
			var leaderIndex = new Random().Next(0, nodeCount);
			if (messageTimeout == -1)
				messageTimeout = Debugger.IsAttached ? 60*1000 : 1000;
			var nodeNames = new string[nodeCount];
			for (int i = 0; i < nodeCount; i++)
			{
				nodeNames[i] = "node" + i;
			}

			for (int index = 0; index < nodeNames.Length; index++)
			{
				var nodeName = nodeNames[index];
				var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
				if (leaderIndex == index)
				{
					storageEnvironmentOptions.OwnsPagers = false;
					PersistentState.ClusterBootstrap(storageEnvironmentOptions);
					storageEnvironmentOptions.OwnsPagers = true;
				}
				var options = CreateNodeOptions(nodeName, messageTimeout, storageEnvironmentOptions,nodeNames);
				_nodes.Add(new RaftEngine(options));
			}
			
			var raftEngine = _nodes[leaderIndex];

			var nopCommit = WaitForCommitsOnCluster(1); // nop commit

			var transport = (InMemoryTransportHub.InMemoryTransport)_inMemoryTransportHub.CreateTransportFor(raftEngine.Name);
			transport.ForceTimeout();
			if (waitForLeader)
			{
				Assert.True(_nodes[leaderIndex].WaitForLeader());

				nopCommit.Wait();
				var leader = _nodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
				Assert.NotNull(leader);
			}

			return _nodes[leaderIndex];
		}

		private RaftEngineOptions CreateNodeOptions(string nodeName,int messageTimeout, StorageEnvironmentOptions storageOptions, params string[] peers)
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
			var raftEngine = new RaftEngine(CreateNodeOptions("node" + _nodes.Count, leader.Options.MessageTimeout, StorageEnvironmentOptions.CreateMemoryOnly()));
			_nodes.Add(raftEngine);
			return raftEngine;
		}

		public virtual void Dispose()
		{
			_nodes.ForEach(node => node.Dispose());
		}
	}
}