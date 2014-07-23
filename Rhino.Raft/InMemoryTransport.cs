using System.Collections.Concurrent;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft
{
	public class InMemoryTransport : ITransport
	{
		private readonly ConcurrentDictionary<string,BlockingCollection<MessageEnvelope>> _messageQueue = new ConcurrentDictionary<string, BlockingCollection<MessageEnvelope>>();

		private void AddToQueue<T>(string dest, T message)
		{
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

		public bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken, out MessageEnvelope messageEnvelope)
		{			
			var messageQueue = _messageQueue.GetOrAdd(dest, s => new BlockingCollection<MessageEnvelope>());
			return messageQueue.TryTake(out messageEnvelope, timeout, cancellationToken);
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
