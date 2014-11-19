// -----------------------------------------------------------------------
//  <copyright file="HttpTransportSender.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Transport
{
	/// <summary>
	/// All requests are fire & forget, with the reply coming in (if at all)
	/// from the resulting thread.
	/// </summary>
	public class HttpTransportSender  : IDisposable
	{
		private readonly ConcurrentDictionary<string, NodeConnectionInfo> _nodeConnectionInfos =
			new ConcurrentDictionary<string, NodeConnectionInfo>();

		private readonly ConcurrentQueue<HttpClient> _httpClientsCache = new ConcurrentQueue<HttpClient>(); 

		public void Stream(string dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, CanInstallSnapshotRequest req)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, TimeoutNowRequest req)
		{
			NodeConnectionInfo info;
			if (_nodeConnectionInfos.TryGetValue(dest, out info) == false)
				throw new InvalidOperationException("Don't know how to connect to " + dest);

			HttpClient result;
			if (_httpClientsCache.TryDequeue(out result) == false)
			{
				result = new HttpClient();
			}

			result.GetAsync(info.Url + "/timeoutNow?term=" + req.Term + "&from=" + req.From);
		}

		public void Send(string dest, AppendEntriesRequest req)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, RequestVoteRequest req)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			
		}
	}
}