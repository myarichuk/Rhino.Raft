using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
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
		private readonly Dictionary<string,NodeConnectionInfo> _peersConnections;
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
			_peersConnections = options.PeersConnections.ToDictionary(x => x.NodeName,x => x);
			_serializer= new JsonSerializer();
			_cancellationTokenSource = new CancellationTokenSource();			
			_httpListener = new HttpListener
			{
				Prefixes =
				{
					"http://+:" + options.CurrentNodeConnection.Port + "/"
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
			Send<CanInstallSnapshotRequest>(dest, req);
		}

		public void Send(string dest, CanInstallSnapshotResponse resp)
		{
			Send<CanInstallSnapshotResponse>(dest, resp);
		}

		public void Send(string dest, InstallSnapshotResponse resp)
		{
			Send<InstallSnapshotResponse>(dest, resp);
		}

		public void Send(string dest, AppendEntriesRequest req)
		{
			Send<AppendEntriesRequest>(dest, req);
		}

		public void Send(string dest, RequestVoteRequest req)
		{
			Send<RequestVoteRequest>(dest, req);
		}

		public void Send(string dest, AppendEntriesResponse resp)
		{
			Send<AppendEntriesResponse>(dest, resp);
		}

		public void Send(string dest, RequestVoteResponse resp)
		{
			Send<RequestVoteResponse>(dest, resp);
		}

		public void Execute(string dest, Action action)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			_logWriter.Write("--== starting disposal of http transport ==--");

			_httpListener.Stop();
			_httpListener.Close();
			_logWriter.Write("--== stopped http listener ==--");

			if (!_messageReceiverThread.Wait(Default.DisposalTimeoutMs, _cancellationTokenSource.Token))
				_logWriter.Write("--== Could not stop Http transport message thread within allocated timeout ( {0}ms ). Probably something went wrong ==--", Default.DisposalTimeoutMs);
		}

		//------------ helpers

		private void Send<TMessage>(string dest, TMessage message)
			where TMessage : class
		{
			var envelope = new MessageEnvelope
			{
				Destination = dest,
				Message = message
			};

			using(var stream = new MemoryStream())
			using(var streamWriter = new StreamWriter(stream))
			using (var jsonWriter = new JsonTextWriter(streamWriter))
			{
				_serializer.Serialize(jsonWriter,envelope);
				using (var client = new HttpClient())
				{
					NodeConnectionInfo peerConnection;
					if (!_peersConnections.TryGetValue(dest, out peerConnection))
						throw new ApplicationException(String.Format("The node '{0}' is not in the peer list. Cannot send message to a node *NOT* in the peer list",dest));

					client.PostAsync(peerConnection.GetUriForMessageSending<TMessage>(), new StreamContent(stream),
										_cancellationTokenSource.Token).Wait(_cancellationTokenSource.Token);
				}
			}
		}
	
	}

	public class HttpTransportOptions
	{
		public NodeConnectionInfo CurrentNodeConnection { get; set; }
		public List<NodeConnectionInfo> PeersConnections { get; set; }
	}
}
