using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Consensus.Raft;
using Rhino.Raft.Messages;
using Voron.Impl;

namespace Rhino.Raft
{
	public class InMemoryTransport : ITransport, IDisposable
	{
		private readonly string _source;
		private readonly ConcurrentDictionary<Type, List<dynamic>>  _handlersByType;
		private readonly BlockingCollection<MessageEnvelope> _messageQueue;
		private readonly CancellationTokenSource _cancellationTokenSource;

		public InMemoryTransport(string source) : this(source, Default.HeartbeatTimeout)
		{
		}

		public InMemoryTransport(string source, TimeSpan heartbeatTimeout)
		{
			if (String.IsNullOrWhiteSpace(source)) throw new ArgumentNullException("source");

			_source = source;
			HeartbeatTimeout = heartbeatTimeout;
			_cancellationTokenSource = new CancellationTokenSource();
			_handlersByType = new ConcurrentDictionary<Type, List<dynamic>>();
			_messageQueue = new BlockingCollection<MessageEnvelope>();


			Task.Run(() => DispatchMessages(), _cancellationTokenSource.Token);
		}

		public TimeSpan HeartbeatTimeout { get; set; }

		public void Send<T>(string dest, T message)
		{
			_messageQueue.Add(new MessageEnvelope { Destination = dest, Message = message, Source = _source});
		}

		public void Send<T>(T message)
		{
			_messageQueue.Add(new MessageEnvelope { Destination = null, Message = message, Source = _source });
		}	

		private void DispatchMessages()
		{
			while (!_messageQueue.IsAddingCompleted)
			{
				_cancellationTokenSource.Token.ThrowIfCancellationRequested();
				MessageEnvelope messageEnvelope;
				if (_messageQueue.TryTake(out messageEnvelope, HeartbeatTimeout))
				{
					
					List<dynamic> observers;
					var type = messageEnvelope.Message.GetType();
					if (_handlersByType.TryGetValue(type, out observers))
					{
						dynamic message = Convert.ChangeType(messageEnvelope.Message, type);
						lock (observers)
						{
							var handlers = observers.Where(x => String.IsNullOrWhiteSpace(x.Name) == false);
							if (messageEnvelope.Destination != null)
								handlers = handlers.Where(x => String.Compare(x.Name, messageEnvelope.Destination, true) == 0);								

							foreach (var handler in handlers)
							{
								handler.Handle(messageEnvelope.Destination, message);
							}
						}
					}
				}
			}
		}


		public void Register<T>(IHandler<T> messageHandler)
		{
			if (messageHandler == null) throw new ArgumentNullException("messageHandler");

			_handlersByType.AddOrUpdate(typeof (T),
				new List<dynamic> { messageHandler },
				(type, observers) =>
				{
					lock (observers)
					{
						observers.Add(messageHandler);
						return observers;
					}
				});
		}

		public void Unregister<T>(IHandler<T> messageHandler)
		{
			List<dynamic> handlers;
			if (_handlersByType.TryGetValue(typeof (T), out handlers))
			{
				lock (handlers)
				{
					handlers.Remove(messageHandler);
				}
			}
		}

		public void Dispose()
		{
			_messageQueue.CompleteAdding();
		}
	}
}
