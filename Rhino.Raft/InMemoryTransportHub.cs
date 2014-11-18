using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft
{
	public class InMemoryTransportHub
	{
		private readonly ConcurrentDictionary<string, BlockingCollection<MessageContext>> _messageQueue =
			new ConcurrentDictionary<string, BlockingCollection<MessageContext>>();

		private readonly HashSet<string> _disconnectedNodes = new HashSet<string>();

		private readonly HashSet<string> _disconnectedNodesFromSending = new HashSet<string>();

		public ConcurrentDictionary<string, BlockingCollection<MessageContext>> MessageQueue
		{
			get { return _messageQueue; }
		}

		public ITransport CreateTransportFor(string from)
		{
			return new InMemoryTransport(this, from);
		}

		public class InMemoryTransport : ITransport
		{
			private readonly InMemoryTransportHub _parent;
			private readonly string _from;

			public InMemoryTransport(InMemoryTransportHub parent, string @from)
			{
				_parent = parent;
				_from = @from;
			}

			public string From
			{
				get { return _from; }
			}

			public bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext)
			{
				return _parent.TryReceiveMessage(_from, timeout, cancellationToken, out messageContext);
			}

			public void Stream(string dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter)
			{
				var stream = new MemoryStream();
				streamWriter(stream);
				stream.Position = 0;

				_parent.AddToQueue(this, dest, snapshotRequest, stream);
			}

			public void Send(string dest, CanInstallSnapshotRequest req)
			{
				_parent.AddToQueue(this, dest, req);
			}

			public void SendInternal(string dest, string from, object msg)
			{
				_parent.AddToQueue(this, dest, msg);
			}

			public void Send(string dest, TimeoutNowRequest req)
			{
				_parent.AddToQueue(this, dest, req);
			}

			public void Send(string dest, AppendEntriesRequest req)
			{
				_parent.AddToQueue(this, dest, req);
			}

			public void Send(string dest, RequestVoteRequest req)
			{
				_parent.AddToQueue(this, dest, req);
			}

			public void SendToSelf(AppendEntriesResponse resp)
			{
				_parent.AddToQueue(this, From, resp);
			}

			public void ForceTimeout(string name)
			{
				_parent.AddToQueue(this, From, new TimeoutException(), evenIfDisconnected: true);
			}
		}

		private void AddToQueue<T>(InMemoryTransport src, string dest, T message, Stream stream = null,
			bool evenIfDisconnected = false)
		{
			//if destination is considered disconnected --> drop the message so it never arrives
			if ((
				_disconnectedNodes.Contains(dest) ||
				_disconnectedNodesFromSending.Contains(src.From)
				) && evenIfDisconnected == false)
				return;

			var newMessage = new InMemoryMessageContext(src)
			{
				Destination = dest,
				Message = message,
				Stream = stream
			};

			_messageQueue.AddOrUpdate(dest, new BlockingCollection<MessageContext> { newMessage },
				(destination, envelopes) =>
				{
					envelopes.Add(newMessage);
					return envelopes;
				});
		}

		private class InMemoryMessageContext : MessageContext
		{
			private readonly InMemoryTransport _parent;

			public InMemoryMessageContext(InMemoryTransport parent)
			{
				_parent = parent;
			}

			public override void Reply(CanInstallSnapshotResponse resp)
			{
				_parent.SendInternal(_parent.From, Destination, resp);
			}

			public override void Reply(InstallSnapshotResponse resp)
			{
				_parent.SendInternal(_parent.From, Destination, resp);
			}

			public override void Reply(AppendEntriesResponse resp)
			{
				_parent.SendInternal(_parent.From, Destination, resp);
			}

			public override void Reply(RequestVoteResponse resp)
			{
				_parent.SendInternal(_parent.From, Destination, resp);
			}

			public override void ExecuteInEventLoop(Action action)
			{
				_parent.SendInternal(_parent.From, _parent.From, action);
			}
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

		public bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken,
			out MessageContext messageContext)
		{
			if (timeout < 0)
				timeout = 0;

			var messageQueue = _messageQueue.GetOrAdd(dest, s => new BlockingCollection<MessageContext>());
			var tryReceiveMessage = messageQueue.TryTake(out messageContext, timeout, cancellationToken);
			if (tryReceiveMessage)
			{
				if (_disconnectedNodes.Contains(dest) ||
					messageContext.Message is TimeoutException)
				{
					messageContext = null;
					return false;
				}
			}

			return tryReceiveMessage;
		}
	}
}
