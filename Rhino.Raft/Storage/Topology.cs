using System;
using System.Collections.Generic;
using System.Linq;
using Rhino.Raft.Transport;

namespace Rhino.Raft.Storage
{
	public class Topology
	{
		private readonly Dictionary<string, NodeConnectionInfo> _allNodes;
		private readonly Dictionary<string, NodeConnectionInfo> _allVotingNodes;
		private readonly Dictionary<string, NodeConnectionInfo> _nonVotingNodes;
		private readonly Dictionary<string, NodeConnectionInfo> _promotableNodes;
		private string _topologyString;

		public Topology()
		{
			_allVotingNodes = new Dictionary<string, NodeConnectionInfo>(StringComparer.OrdinalIgnoreCase);
			_nonVotingNodes = new Dictionary<string, NodeConnectionInfo>(StringComparer.OrdinalIgnoreCase);
			_promotableNodes = new Dictionary<string, NodeConnectionInfo>(StringComparer.OrdinalIgnoreCase);
			_allNodes = new Dictionary<string, NodeConnectionInfo>(StringComparer.OrdinalIgnoreCase);
		}

		public Topology(IEnumerable<NodeConnectionInfo> allVotingNodes, IEnumerable<NodeConnectionInfo> nonVotingNodes,
			IEnumerable<NodeConnectionInfo> promotableNodes)
			: this()
		{
			foreach (NodeConnectionInfo nodeConnectionInfo in allVotingNodes)
			{
				_allVotingNodes[nodeConnectionInfo.Name] = nodeConnectionInfo;
				_allNodes[nodeConnectionInfo.Name] = nodeConnectionInfo;
			}
			foreach (NodeConnectionInfo nodeConnectionInfo in nonVotingNodes)
			{
				_nonVotingNodes[nodeConnectionInfo.Name] = nodeConnectionInfo;
				_allNodes[nodeConnectionInfo.Name] = nodeConnectionInfo;
			}
			foreach (NodeConnectionInfo nodeConnectionInfo in promotableNodes)
			{
				_promotableNodes[nodeConnectionInfo.Name] = nodeConnectionInfo;
				_allNodes[nodeConnectionInfo.Name] = nodeConnectionInfo;
			}


			CreateTopologyString();
		}

		public IEnumerable<NodeConnectionInfo> AllVotingNodes
		{
			get { return _allVotingNodes.Values; }
		}

		public IEnumerable<NodeConnectionInfo> NonVotingNodes
		{
			get { return _nonVotingNodes.Values; }
		}

		public IEnumerable<NodeConnectionInfo> PromotableNodes
		{
			get { return _promotableNodes.Values; }
		}

		public int QuorumSize
		{
			get { return (_allVotingNodes.Count/2) + 1; }
		}

		public IEnumerable<string> AllNodeNames
		{
			get { return _allNodes.Keys; }
		}

		public IEnumerable<NodeConnectionInfo> AllNodes
		{
			get { return _allNodes.Values; }
		} 

		public bool HasVoters
		{
			get { return _allVotingNodes.Count > 0; }
		}

		private void CreateTopologyString()
		{
			if (_allNodes.Count == 0)
			{
				_topologyString = "<empty topology>";
				return;
			}

			_topologyString = "";
			if (_allVotingNodes.Count > 0)
				_topologyString += "Voting: [" + string.Join(", ", _allVotingNodes.Keys) + "] ";
			if (_nonVotingNodes.Count > 0)
				_topologyString += "Non voting: [" + string.Join(", ", _nonVotingNodes.Keys) + "] ";
			if (_promotableNodes.Count > 0)
				_topologyString += "Promotables: [" + string.Join(", ", _promotableNodes.Keys) + "] ";
		}

		public bool HasQuorum(HashSet<string> votes)
		{
			return votes.Count(IsVoter) >= QuorumSize;
		}

		public override string ToString()
		{
			if (_topologyString == null)
				CreateTopologyString();
			return _topologyString;
		}

		public bool Contains(string node)
		{
			return _allVotingNodes.ContainsKey(node) || _nonVotingNodes.ContainsKey(node) || _promotableNodes.ContainsKey(node);
		}

		public bool IsVoter(string node)
		{
			return _allVotingNodes.ContainsKey(node);
		}

		public bool IsPromotable(string node)
		{
			return _promotableNodes.ContainsKey(node);
		}

		public NodeConnectionInfo GetNodeByName(string node)
		{
			NodeConnectionInfo info;
			_allNodes.TryGetValue(node, out info);
			return info;
		}
	}
}