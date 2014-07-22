using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
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

			public string Name { get; set; }

			public bool HasReceivedMessage { get; private set; }

			public Action<string,TMessage> ValidateMessage { get; set; }
			public void Handle(string source, TMessage message)
			{
				HasReceivedMessage = true;
				ValidateMessage(source, message);
			}

		}

		[Fact]
		public void Subscriber_should_receive_messages_in_order()
		{
			var messages = new List<MessageA>();
			for (int i = 0; i < 250; i++)
				messages.Add(new MessageA{ Id = "A/" + i });

			var receivedMessages = new ConcurrentQueue<MessageA>();
			var completedEvent = new ManualResetEventSlim();

			var subscriber = new Subscriber<MessageA>
			{
				Name = "A",
				ValidateMessage = (destination, message) =>
				{
					receivedMessages.Enqueue(message);
					if(receivedMessages.Count == messages.Count)
						completedEvent.Set();
				}
			};
			using (var transport = new InMemoryTransport("sourceNode"))
			{
				transport.RegisterHandler(subscriber);
				
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
			using (var transport = new InMemoryTransport("sourceNode"))
			{
				var messageReceivedEvent = new CountdownEvent(2);
				var subscriber1 = new Subscriber<MessageA>
				{
					Name = "A",
					ValidateMessage = (source, message) =>
					{
						Assert.Equal("ABC",message.Id);
						messageReceivedEvent.Signal();
					}
				};

				var subscriber2 = new Subscriber<MessageA>
				{
					Name = "A",
					ValidateMessage = (source, message) =>
					{
						Assert.Equal("ABC", message.Id);
						messageReceivedEvent.Signal();						
					}
				};

				transport.RegisterHandler(subscriber1);
				transport.RegisterHandler(subscriber2);

				transport.Send("A", new MessageB { Id = "BCD" });
				transport.Send("A", new MessageA { Id = "ABC" });

				Assert.True(messageReceivedEvent.Wait(20000));

				Assert.True(subscriber1.HasReceivedMessage);
				Assert.True(subscriber2.HasReceivedMessage);
			}
		}
	}
}
