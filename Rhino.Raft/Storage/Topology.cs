using System;
using System.Collections.Generic;
using System.Linq;

namespace Rhino.Raft.Storage
{
	public class Topology
	{
		public IEnumerable<string> AllVotingNodes
		{
			get { return _allVotingNodes; }
		}

		public IEnumerable<string> NonVotingNodes
		{
			get { return _nonVotingNodes; }
		}

		public IEnumerable<string> PromotableNodes
		{
			get { return _promotableNodes; }
		}

		private string _topologyString;
		private readonly HashSet<string> _allVotingNodes;
		private readonly HashSet<string> _nonVotingNodes;
		private readonly HashSet<string> _promotableNodes;

		private readonly HashSet<string> _allNodes;

		public int QuorumSize
		{
			get { return (_allVotingNodes.Count / 2) + 1; }
		}

		public IEnumerable<string> AllNodes
		{
			get { return _allNodes; }
		}

		public bool HasVoters
		{
			get { return _allVotingNodes.Count > 0; }
		}

		public Topology()
		{
			_allVotingNodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			_nonVotingNodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			_promotableNodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			_allNodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		}

		public Topology(IEnumerable<string> allVotingNodes, IEnumerable<string> nonVotingNodes, IEnumerable<string> promotableNodes)
			: this()
		{
			_allVotingNodes.UnionWith(allVotingNodes);
			_nonVotingNodes.UnionWith(nonVotingNodes);
			_promotableNodes.UnionWith(promotableNodes);

			_allNodes.UnionWith(_allVotingNodes);
			_allNodes.UnionWith(_nonVotingNodes);
			_allNodes.UnionWith(_promotableNodes);

			CreateTopologyString();
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
				_topologyString += "Voting: [" + string.Join(", ", _allVotingNodes) + "] ";
			if (_nonVotingNodes.Count > 0)
				_topologyString += "Non voting: [" + string.Join(", ", _nonVotingNodes) + "] ";
			if (_promotableNodes.Count > 0)
				_topologyString += "Promotables: [" + string.Join(", ", _promotableNodes) + "] ";
		}


		public bool HasQuorum(HashSet<string> votes)
		{
			var sum = AllVotingNodes.Count(votes.Contains);
			return sum >= QuorumSize;
		}

		public override string ToString()
		{
			if (_topologyString == null)
				CreateTopologyString();
			return _topologyString;
		}

		public bool Contains(string node)
		{
			return _allVotingNodes.Contains(node) || _nonVotingNodes.Contains(node) || _promotableNodes.Contains(node);
		}

		public bool IsVoter(string node)
		{
			return _allVotingNodes.Contains(node);
		}

		public bool IsPromotable(string node)
		{
			return _promotableNodes.Contains(node);
		}
	}
}