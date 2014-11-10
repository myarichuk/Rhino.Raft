using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;

namespace Rhino.Raft
{
	public class InMemoryTransport : ITransport
	{
		private readonly ConcurrentDictionary<string,BlockingCollection<MessageEnvelope>> _messageQueue = new ConcurrentDictionary<string, BlockingCollection<MessageEnvelope>>();

		private readonly HashSet<string> _disconnectedNodes = new HashSet<string>();

		private readonly HashSet<string> _disconnectedNodesFromSending = new HashSet<string>();

		public ConcurrentDictionary<string, BlockingCollection<MessageEnvelope>> MessageQueue
		{
			get { return _messageQueue; }
		}

		private void AddToQueue<T>(string dest, T message)
		{
			//if destination is considered disconnected --> drop the message so it never arrives
			if(_disconnectedNodes.Contains(dest))
				return;

			var newMessage = new MessageEnvelope
			{
				Destination = dest,
				Message = message
			};

			_messageQueue.AddOrUpdate(dest,new BlockingCollection<MessageEnvelope> { newMessage }, 
			(destination, envelopes) =>
			{
				envelopes.Add(newMessage);
				return envelopes;
			} );
		}

		public void DisconnectNodeSending(string node)
		{
			_disconnectedNodesFromSending.Add(node);
		}

		public void ReconnectNodeSending(string node)
		{
			_disconnectedNodesFromSending.RemoveWhere(n => n.Equals(node, StringComparison.InvariantCultureIgnoreCase));
		}

		public void DisconnectNode(string node)
		{
			_disconnectedNodes.Add(node);
		}

		public void ReconnectNode(string node)
		{
			_disconnectedNodes.RemoveWhere(n => n.Equals(node, StringComparison.InvariantCultureIgnoreCase));
		}

		public bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken, out MessageEnvelope messageEnvelope)
		{
			messageEnvelope = null;
			if (_disconnectedNodes.Contains(dest))
				return false;

		    if (timeout < 0)
		        timeout = 0;

			var messageQueue = _messageQueue.GetOrAdd(dest, s => new BlockingCollection<MessageEnvelope>());
            return messageQueue.TryTake(out messageEnvelope, timeout, cancellationToken);
		}

	    public void Stream(string dest, InstallSnapshotRequest req, Action<Stream> stream)
	    {
			if (_disconnectedNodesFromSending.Contains(req.From))
				return;
            AddToQueue(dest, req);
        }

	    public void Send(string dest, AppendEntriesRequest req)
		{
			if (_disconnectedNodes.Contains(req.LeaderId) || _disconnectedNodesFromSending.Contains(req.From))
				return;
			AddToQueue(dest, req);
		}

		public void Send(string dest, RequestVoteRequest req)
		{
			if (_disconnectedNodes.Contains(req.CandidateId) || _disconnectedNodesFromSending.Contains(req.From))
				return;
			AddToQueue(dest, req);
		}

		public void Send(string dest, AppendEntriesResponse resp)
		{
			if (_disconnectedNodesFromSending.Contains(resp.From))
				return;
			AddToQueue(dest, resp);
		}

		public void Send(string dest, RequestVoteResponse resp)
		{
			if (_disconnectedNodesFromSending.Contains(resp.From))
				return;
			AddToQueue(dest, resp);
		}

	}
}
