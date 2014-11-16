using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Utils;

namespace Rhino.Raft
{
	public class HttpTransport : ITransport, IDisposable
	{
		private readonly DebugWriter _logWriter;

		private readonly NodeConnectionInfo _currentNodeConnection;
		private readonly List<NodeConnectionInfo> _peersConnections;
		private readonly JsonSerializer _serializer; 
		
		private readonly Task _messageReceiverThread;

		private readonly CancellationTokenSource _cancellationTokenSource;

		private readonly BlockingCollection<HttpListenerContext> _requests = new BlockingCollection<HttpListenerContext>(Default.MaxIncomingConcurrentRequests);

		//---- comm stuff
		private readonly HttpListener _httpListener;
		public HttpTransport(HttpTransportOptions options, DebugWriter logWriter)
		{
			_logWriter = logWriter;
			if (options.CurrentNodeConnection == null) throw new ArgumentNullException("currentNodeConnection");
			if (options.PeersConnections == null) throw new ArgumentNullException("peersConnections");

			_currentNodeConnection = options.CurrentNodeConnection;
			_peersConnections = options.PeersConnections.ToList();
			_serializer= new JsonSerializer();
			_cancellationTokenSource = new CancellationTokenSource();			
			_httpListener = new HttpListener
			{
				Prefixes =
				{
					"http://+:" + options.Port + "/"
				}
			};
			_httpListener.Start();

			_messageReceiverThread = new Task(RunMessageLoop, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning);
			_messageReceiverThread.Start();
			_logWriter.Write("--== Http Transport initialized ==--");
		}

		private void RunMessageLoop()
		{
			_logWriter.Write("--== Started Listening Thread ==--");
			while (_httpListener.IsListening)
			{
				var ctx = _httpListener.GetContext();
				_requests.Add(ctx);
				_logWriter.Write("--== Incoming request from {0}, adding to incoming cache ==--", ctx.Request.RawUrl);
			}
		}

		public bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken,
			out MessageEnvelope messageEnvelope)
		{
			messageEnvelope = null;

			HttpListenerContext ctx;
			if (_requests.TryTake(out ctx, timeout, cancellationToken) == false)
			{
				_logWriter.Write("--== Http transport timed-out while trying to receive message. Request url was {0} ==--", ctx.Request.RawUrl);
				return false;
			}

			try
			{
				using (var requestReader = new StreamReader(ctx.Request.InputStream))
				using (var jsonReader = new JsonTextReader(requestReader))
					messageEnvelope = _serializer.Deserialize<MessageEnvelope>(jsonReader);
			}
			catch (Exception e)
			{
				throw new ApplicationException("Failed to deserialize a message. This should not happen - it is probably a bug. Exception thrown: " + e);
			}


			_logWriter.Write("--== Http transport received a message. Request url was {0} ==--", ctx.Request.RawUrl);			
			return true;
		}

		public void Stream(string dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, CanInstallSnapshotRequest req)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, CanInstallSnapshotResponse resp)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, InstallSnapshotResponse resp)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, AppendEntriesRequest req)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, RequestVoteRequest req)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, AppendEntriesResponse resp)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, RequestVoteResponse resp)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			_logWriter.Write("--== starting disposal of http transport ==--");

			_httpListener.Stop();
			_httpListener.Close();
			_logWriter.Write("--== stopped http listener ==--");
		}

		//------------ helpers

		private bool TryGetDestinationInfo(string dest,out NodeConnectionInfo peerDestinationInfo)
		{
			peerDestinationInfo = _peersConnections.FirstOrDefault(peerConnection => peerConnection.NodeName == dest);
			return peerDestinationInfo == null;
		}
	}

	public class HttpTransportOptions
	{
		public NodeConnectionInfo CurrentNodeConnection { get; set; }
		public List<NodeConnectionInfo> PeersConnections { get; set; }
		public short Port { get; set; }

		public HttpTransportOptions()
		{
			Port = Default.HttpTransportListeningPort;
		}
	}
}
