using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Implementations.Transports;
using Rhino.Raft.Interfaces;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class InMemoryTransportTest
	{
		public class MessageA
		{
			public string Id { get; set; }
		}

		public class MessageB
		{
			public string Id { get; set; }
		}

		public class Subscriber<TMessage> : IHandler<TMessage>
		{
			public Subscriber()
			{
				HasReceivedMessage = false;
			}

			public bool HasReceivedMessage { get; private set; }

			public Action<string,TMessage> ValidateMessage { get; set; }
			public void Receive(string source, TMessage message)
			{
				HasReceivedMessage = true;
				ValidateMessage(source, message);
			}
		}

		[Fact]
		public void Subscriber_should_receive_messages_in_order()
		{
			var messages = new List<MessageA>();
			for (int i = 0; i < 1000; i++)
				messages.Add(new MessageA{ Id = "A/" + i });

			var receivedMessages = new ConcurrentQueue<MessageA>();
			var completedEvent = new ManualResetEventSlim();

			var subscriber = new Subscriber<MessageA>
			{
				ValidateMessage = (source, message) =>
				{
					receivedMessages.Enqueue(message);
					if(receivedMessages.Count == messages.Count)
						completedEvent.Set();
				}
			};
			using (var transport = new InMemoryTransport())
			{
				transport.Subscribe(subscriber);
				
				foreach(var message in messages)
					transport.Send("A",message);

				completedEvent.Wait();
			}

			int counter = 0;
			foreach (var message in receivedMessages.ToArray())
			{
				Assert.Equal("A/" + (counter++),message.Id);
			}
		}

		[Fact]
		public void All_subscribers_should_receive_messages_by_type()
		{
			using (var transport = new InMemoryTransport())
			{
				var messageReceivedEvent = new CountdownEvent(2);
				var subscriber1 = new Subscriber<MessageA>
				{
					ValidateMessage = (source, message) =>
					{
						Assert.Equal("SourceNode1", source);
						Assert.Equal("ABC",message.Id);
						messageReceivedEvent.Signal();
					}
				};

				var subscriber2 = new Subscriber<MessageA>
				{
					ValidateMessage = (source, message) =>
					{
						Assert.Equal("SourceNode1", source);
						Assert.Equal("ABC", message.Id);
						messageReceivedEvent.Signal();						
					}
				};

				transport.Subscribe(subscriber1);
				transport.Subscribe(subscriber2);

				transport.Send("SourceNode1", new MessageB { Id = "BCD" });
				transport.Send("SourceNode1", new MessageA { Id = "ABC" });

				Assert.True(messageReceivedEvent.Wait(3000));

				Assert.True(subscriber1.HasReceivedMessage);
				Assert.True(subscriber2.HasReceivedMessage);
			}
		}
	}
}
