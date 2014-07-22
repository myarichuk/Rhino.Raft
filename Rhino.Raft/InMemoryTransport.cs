using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft
{
	public class InMemoryTransport : ITransport
	{
		private readonly BlockingCollection<MessageEnvelope> _messageQueue = new BlockingCollection<MessageEnvelope>();

		private void AddToQueue<T>(string dest, T message)
		{
			_messageQueue.Add(new MessageEnvelope
			{
				Destination = dest,
				Message = message
			});
		}

		public bool TryReceiveMessage(out MessageEnvelope messageEnvelope, int timeout, CancellationToken cancellationToken)
		{
			return _messageQueue.TryTake(out messageEnvelope, timeout, cancellationToken);
		}

		public void Send(string dest, AppendEntriesRequest req)
		{
			AddToQueue(dest, req);
		}

		public void Send(string dest, RequestVoteRequest req)
		{
			AddToQueue(dest, req);
		}

		public void Send(string dest, AppendEntriesResponse resp)
		{
			AddToQueue(dest, resp);
		}

		public void Send(string dest, RequestVoteResponse resp)
		{
			AddToQueue(dest, resp);

		}
	}
}
