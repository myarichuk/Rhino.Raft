using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Voron;

namespace Rhino.Raft.Tests
{
	public class RaftTestsBase : IDisposable
	{
		private readonly List<RaftEngine> _nodes = new List<RaftEngine>();

		protected RaftEngine CreateNodeWithVirtualNetworkAndMakeItLeader(string leaderNodeName, ITransport transport, params string[] virtualPeers)
		{
			var leaderNode = new RaftEngine(CreateNodeOptions(leaderNodeName, transport, 1500, virtualPeers));
			leaderNode.WaitForEventTask(
				(node, handler) => node.ElectionStarted += handler,
				(node, handler) => node.ElectionStarted -= handler).Wait();

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
				(n, handler) => n.EventsProcessed += handler,
				(n, handler) => n.EventsProcessed -= handler);

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

		protected IEnumerable<RaftEngine> CreateNodeNetwork(int nodeCount, ITransport transport = null, int messageTimeout = 1000,Func<RaftEngineOptions,RaftEngineOptions> optionChangerFunc = null)
		{
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