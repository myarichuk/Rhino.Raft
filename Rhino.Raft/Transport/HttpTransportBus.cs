// -----------------------------------------------------------------------
//  <copyright file="HttpTransportBus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Transport
{
	public class HttpTransportBus : IDisposable
	{
		private readonly BlockingCollection<HttpTransportMessageContext> _queue = new BlockingCollection<HttpTransportMessageContext>();

		public bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext)
		{
			HttpTransportMessageContext item;
			if (_queue.TryTake(out item, timeout, cancellationToken) == false)
			{
				messageContext = null;
				return false;
			}
			messageContext = item;
			return true;
		}

		private class HttpTransportMessageContext : MessageContext
		{
			private readonly TaskCompletionSource<HttpResponseMessage> _tcs;
			private readonly HttpTransportBus _parent;

			public HttpTransportMessageContext(TaskCompletionSource<HttpResponseMessage> tcs, HttpTransportBus parent)
			{
				_tcs = tcs;
				_parent = parent;
			}

			private void Reply(bool success, object msg)
			{
				if (_tcs == null)
					return;

				var httpResponseMessage = new HttpResponseMessage(
					success ? HttpStatusCode.OK : HttpStatusCode.NotAcceptable
					);
				if (msg != null)
				{
					httpResponseMessage.Content = new ObjectContent(msg.GetType(), msg, new JsonMediaTypeFormatter());
				}
				_tcs.TrySetResult(httpResponseMessage);
			}

			public override void Reply(CanInstallSnapshotResponse resp)
			{
				Reply(resp.Success, resp);	
			}

			public override void Reply(InstallSnapshotResponse resp)
			{
				Reply(resp.Success, resp);	
			}

			public override void Reply(AppendEntriesResponse resp)
			{
				Reply(resp.Success, resp);	
			}

			public override void Reply(RequestVoteResponse resp)
			{
				Reply(resp.VoteGranted, resp);	
			}

			public override void ExecuteInEventLoop(Action action)
			{
				_parent.Publish(action, null);
			}
		}

		public void SendToSelf(AppendEntriesResponse resp)
		{
			Publish(resp, null);
		}

		public void Publish(object msg, TaskCompletionSource<HttpResponseMessage> source, Stream stream = null)
		{
			_queue.Add(new HttpTransportMessageContext(source, this)
			{
				Message = msg,
				Stream = stream,
			});
		}

		public void Dispose()
		{

		}
	}
}