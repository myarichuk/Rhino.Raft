using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rhino.Raft.Messages;
using Rhino.Raft.Utils;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class HttpTransportTests : IDisposable
	{
		private const short FirstPort = 8100;

		private readonly string _localHost;
		private readonly List<IDisposable> _disposables;

		public HttpTransportTests()
		{
			_disposables = new List<IDisposable>();
			var isFiddlerActive =
				Process.GetProcesses().Any(p => p.ProcessName.Equals("fiddler",StringComparison.InvariantCultureIgnoreCase));
			_localHost = isFiddlerActive ? "localhost.fiddler" : "localhost";
		}
		
		[Fact]
		public void Should_receive_messages_in_the_same_order()
		{
			var transports = SetupTransports(2);
			var messages = GenerateRandomAppendEntryRequests(25);

			Task.Run(() =>
			{
				foreach(var msg in messages)
					transports[0].Send(transports[1].NodeName,msg);
			});

			var receivedMessages = new List<AppendEntriesRequest>();
			for (int i = 0; i < messages.Length; i++)
			{
				MessageEnvelope envelope;
				transports[1].TryReceiveMessage(transports[1].NodeName, 1000, new CancellationTokenSource().Token, out envelope);
				receivedMessages.Add(envelope.Message as AppendEntriesRequest);
			}

			receivedMessages.ShouldBeEquivalentTo(messages,
				options => options.IncludingAllRuntimeProperties()
								  .IncludingNestedObjects()
								  .WithStrictOrdering());
		}

		[Fact]
		public void Can_send_and_receive_message()
		{
			var transports = SetupTransports(2);
			var dummyAppendEntriesRequest = GenerateRandomAppendEntryRequests(1).First();

			//since the Send() will not return until it receives response, it needs to be run in separate thread
// ReSharper disable once AccessToDisposedClosure

			MessageEnvelope envelope = null;
			bool hasTakenMessage = false;
			Assert.DoesNotThrow(() =>
			{
				Task.Run(() => transports[0].Send(transports[1].NodeName, dummyAppendEntriesRequest));
				hasTakenMessage = transports[1].
					TryReceiveMessage(transports[1].NodeName, 
					Default.SendTimeout, 
					new CancellationTokenSource().Token, out envelope);
			});

			hasTakenMessage.Should().BeTrue();
			envelope.Should().NotBeNull();

			envelope.Message.ShouldBeEquivalentTo(dummyAppendEntriesRequest,
				//IncludingAllRuntimeProperties --> envelope.Message property is of type Object
				//IncludingNestedObjects --> will compare LogEntry properties inside of Entries array
				opts => opts.IncludingAllRuntimeProperties()
							.IncludingNestedObjects());

		}

		private HttpTransport[] SetupTransports(int nodeCount, string namePrefix = "test")
		{
			var transports = new List<HttpTransport>();
			var connections = new List<NodeConnectionInfo>();
			for (short i = 0; i < nodeCount; i++)
			{
				var connectionInfo = new NodeConnectionInfo
				{
					Url = _localHost,
					Port = (short)(FirstPort + i),
					NodeName = namePrefix + i
				};
				connections.Add(connectionInfo);
			}

			int x = 0;
			foreach (var currentConnection in connections)
			{
				var peerConnections = connections.Where(c => c != currentConnection).ToList();
				HttpTransport transport;
				do
				{
				} while (!TryInitializeHttpTransport(currentConnection, peerConnections, out transport));
				transports.Add(transport);
			}

			_disposables.AddRange(transports);
			return transports.ToArray();
		}

		private bool TryInitializeHttpTransport(NodeConnectionInfo currentConnection, List<NodeConnectionInfo> peerConnections, out HttpTransport transport)
		{
			try
			{
				transport = new HttpTransport(new HttpTransportOptions
				{
					CurrentNodeConnection = currentConnection,
					PeersConnections = peerConnections
				}, new DebugWriter(currentConnection.NodeName, new Stopwatch()));
			}
			catch (HttpListenerException)
			{
				transport = null;
				return false;
			}
			return true;
		}

		private byte[] RandomBytes(int size)
		{
			var bytes = new List<byte>();
			var generator = new RandomGenerator();
			for (int i = 0; i < size; i++)
				bytes.Add(generator.Next(byte.MinValue,byte.MaxValue));

			return bytes.ToArray();
		}

		private LogEntry[] GenerateRandomLogEntries(int size)
		{
			return Builder<LogEntry>.CreateListOfSize(size)
				.All()
				.With(x => x.Data = RandomBytes(256))
				.Build().ToArray();
		}

// ReSharper disable once ReturnTypeCanBeEnumerable.Local
		private AppendEntriesRequest[] GenerateRandomAppendEntryRequests(int size)
		{
			return Builder<AppendEntriesRequest>.CreateListOfSize(size)
				.All()
				.With(x => x.Entries = GenerateRandomLogEntries(10))
				.Build().ToArray();
		}

		public void Dispose()
		{
			foreach(var disposable in _disposables)
				disposable.Dispose();
		}
	}
}
