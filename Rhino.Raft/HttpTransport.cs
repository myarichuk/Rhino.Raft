using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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

		public string NodeName
		{
			get { return _currentNodeConnection.NodeName; }
		}

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
				},			
			};
			_httpListener.Start();

			_messageReceiverThread = new Task(RunMessageLoop, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning);
			_messageReceiverThread.Start();
			_logWriter.Write("--== Http Transport initialized ==--");
		}

		private void RunMessageLoop()
		{
			_logWriter.Write("--== Started Listening Thread ==--");
			while (_httpListener.IsListening && !_cancellationTokenSource.IsCancellationRequested)
			{				
				var ctx = _httpListener.GetContext();
				_requests.Add(ctx);
				_logWriter.Write("--== Incoming request from {0}, adding to incoming cache ==--", ctx.Request.Url);
			}
		}

		public bool TryReceiveMessage(string dest, int timeout, CancellationToken cancellationToken,
			out MessageEnvelope messageEnvelope)
		{
			messageEnvelope = null;

			HttpListenerContext ctx;
			if (_requests.TryTake(out ctx, timeout, cancellationToken) == false)
			{
				_logWriter.Write("--== Http transport timed-out while trying to receive message. ==--");
				return false;
			}

			try
			{
				using (var requestReader = new StreamReader(ctx.Request.InputStream))
				using (var jsonReader = new JsonTextReader(requestReader))
					messageEnvelope = _serializer.Deserialize<MessageEnvelope>(jsonReader);

				var messageType = Type.GetType("Rhino.Raft.Messages." + ctx.Request.RawUrl.Replace("/", ""),true,true);
				var messageJObject = messageEnvelope.Message as JObject;
				if(messageJObject == null)
					throw new FormatException("Received message with unknown format - it is not a Json object. This should not happen, it is a bug.");

				messageEnvelope.Message = messageJObject.ToObject(messageType);
			}
			catch (Exception e)
			{
				ctx.Response.StatusCode = 500;
				var error = "Failed to deserialize a message. This should not happen - it is probably a bug. Exception thrown: " + e;
				ctx.Response.StatusDescription = error;
				ctx.Response.ContentLength64 = 0;
				ctx.Response.OutputStream.Close();

				throw new ApplicationException(error);				
			}

			
			const string message = "Http transport received a message. Request url was {0} ";
			var successMessage = String.Format(message, ctx.Request.Url);

			_logWriter.Write(successMessage);
			
			ctx.Response.StatusCode = 200;
			ctx.Response.StatusDescription = successMessage;
			ctx.Response.ContentLength64 = 0;
			ctx.Response.OutputStream.Close();

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

		public void Send(string dest, TimeoutNowRequest req)
		{
			Send<TimeoutNowRequest>(dest, req);
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
			_cancellationTokenSource.Cancel();

			_logWriter.Write("--== stopped http listener ==--");

			try
			{
				if (!_messageReceiverThread.Wait(Default.DisposalTimeoutMs, _cancellationTokenSource.Token))
					_logWriter.Write(
						"--== Could not stop Http transport message thread within allocated timeout ( {0}ms ). Probably something went wrong ==--",
						Default.DisposalTimeoutMs);
			}
			catch (OperationCanceledException) { }
			_httpListener.Stop();
			_httpListener.Close();
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

			using (var client = new HttpClient())
			{
				NodeConnectionInfo peerConnection;
				if (!_peersConnections.TryGetValue(dest, out peerConnection))
					throw new ApplicationException(String.Format("The node '{0}' is not in the peer list. Cannot send message to a node *NOT* in the peer list", dest));

				var targetAddress = peerConnection.GetUriForMessageSending<TMessage>();
				var json = JObject.FromObject(envelope).ToString();
				var postAsync = client.PostAsync(targetAddress, new StringContent(json,Encoding.UTF8), _cancellationTokenSource.Token);
				postAsync.Wait(Default.SendTimeout, _cancellationTokenSource.Token);

				var response = postAsync.Result;
				_logWriter.Write("--== sending message of type {0}, response code = {1} (message in the response is {2})", typeof(TMessage).Name, response.StatusCode, response.ReasonPhrase ?? "no explanation sent by the server");
			}
		}
	
	}

	public class HttpTransportOptions
	{
		public NodeConnectionInfo CurrentNodeConnection { get; set; }
		public List<NodeConnectionInfo> PeersConnections { get; set; }
	}
}
