// -----------------------------------------------------------------------
//  <copyright file="HttpTransportSender.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NLog;
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

		private readonly ConcurrentDictionary<string, ConcurrentQueue<HttpClient>> _httpClientsCache = new ConcurrentDictionary<string, ConcurrentQueue<HttpClient>>();
		private readonly Logger _log;
		public HttpTransportSender(string name)
		{
			_log = LogManager.GetLogger(GetType().Name + "." + name);
		}

		public void Stream(string dest, InstallSnapshotRequest req, Action<Stream> streamWriter)
		{
			HttpClient client;
			using (GetConnection(dest, out client))
			{
				LogStatus("install snapshot to " + dest, async () =>
				{
					var requestUri =
						string.Format("/installSnapshot?term={0}&=lastIncludedIndex={1}&lastIncludedTerm={2}&leaderId={3}&from={4}",
							req.Term, req.LastIncludedIndex, req.LastIncludedTerm, req.LeaderId, req.From);
					var httpResponseMessage = await client.PostAsync(requestUri, new SnapshotContent(streamWriter));
					var reply = await httpResponseMessage.Content.ReadAsStringAsync();
					var canInstallSnapshotResponse = JsonConvert.DeserializeObject<CanInstallSnapshotResponse>(reply);
					SendToSelf(canInstallSnapshotResponse);
				});
			}
		}

		public class SnapshotContent : HttpContent
		{
			private readonly Action<Stream> _streamWriter;

			public SnapshotContent(Action<Stream> streamWriter)
			{
				_streamWriter = streamWriter;
			}

			protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				_streamWriter(stream);

				return Task.FromResult(1);
			}

			protected override bool TryComputeLength(out long length)
			{
				length = -1;
				return false;
			}
		}

		public void Send(string dest, AppendEntriesRequest req)
		{
			HttpClient client;
			using (GetConnection(dest, out client))
			{
				LogStatus("can install snapshot to " + dest, async () =>
				{
					var requestUri = string.Format("/appendEntries?term={0}&=leaderCommit{1}&leaderId={2}&prevLogTerm={3}&prevLogIndex={4}&entriesCount={5}&from={6}",
						req.Term, req.LeaderCommit, req.LeaderId, req.PrevLogTerm, req.PrevLogIndex, req.EntriesCount, req.From);
					var httpResponseMessage = await client.PostAsync(requestUri,new EntriesContent(req.Entries));
					var reply = await httpResponseMessage.Content.ReadAsStringAsync();
					var canInstallSnapshotResponse = JsonConvert.DeserializeObject<AppendEntriesResponse>(reply);
					SendToSelf(canInstallSnapshotResponse);
				});
			}
		}

		private class EntriesContent : HttpContent
		{
			private readonly LogEntry[] _entries;

			public EntriesContent(LogEntry[] entries)
			{
				_entries = entries;
			}

			protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				foreach (var logEntry in _entries)
				{
					Write7BitEncodedInt64(stream, logEntry.Index);
					Write7BitEncodedInt64(stream, logEntry.Term);
					stream.WriteByte(logEntry.IsTopologyChange == true ? (byte)1 : (byte)0);
					stream.Write(logEntry.Data, 0, logEntry.Data.Length);
				}
				return Task.FromResult(1);
			}

			private void Write7BitEncodedInt64(Stream stream, long value)
			{
				var v = (ulong)value;
				while (v >= 128)
				{
					stream.WriteByte((byte)(v | 128));
					v >>= 7;
				}
				stream.WriteByte((byte)(v));
			}

			private int SizeOf7BitEncodedInt64(long value)
			{
				var size = 1;
				var v = (ulong)value;
				while (v >= 128)
				{
					size ++;
					v >>= 7;
				}
				return size;
			}

			protected override bool TryComputeLength(out long length)
			{
				length = 0;
				foreach (var logEntry in _entries)
				{
					length += SizeOf7BitEncodedInt64(logEntry.Index) +
					          SizeOf7BitEncodedInt64(logEntry.Term) +
					          1 /*topology*/+
					          logEntry.Data.Length;
				}
				return true;
			}
		}

		public void Send(string dest, CanInstallSnapshotRequest req)
		{
			HttpClient client;
			using (GetConnection(dest, out client))
			{
				LogStatus("can install snapshot to " + dest, async () =>
				{
					var requestUri = string.Format("/canInstallSnapshot?term={0}&=index{1}&leaderId={2}&from={3}", req.Term, req.Index,
						req.LeaderId, req.From);
					var httpResponseMessage = await client.GetAsync(requestUri);
					var reply = await httpResponseMessage.Content.ReadAsStringAsync();
					var canInstallSnapshotResponse = JsonConvert.DeserializeObject<CanInstallSnapshotResponse>(reply);
					SendToSelf(canInstallSnapshotResponse);
				});
			}
		}
	
		public void Send(string dest, RequestVoteRequest req)
		{
			HttpClient client;
			using (GetConnection(dest, out client))
			{
				LogStatus("request vote from " + dest, async () =>
				{
					var requestUri = string.Format("/requestVote?term={0}&=lastLogIndex{1}&lastLogTerm={2}&candidateId={3}&trialOnly={4}&forcedElection={5}&from={6}", 
						req.Term, req.LastLogIndex, req.LastLogTerm, req.CandidateId, req.TrialOnly, req.ForcedElection, req.From);
					var httpResponseMessage = await client.GetAsync(requestUri);
					var reply = await httpResponseMessage.Content.ReadAsStringAsync();
					var canInstallSnapshotResponse = JsonConvert.DeserializeObject<RequestVoteResponse>(reply);
					SendToSelf(canInstallSnapshotResponse);
				});
			}
		}

		private void SendToSelf(object o)
		{
			throw new NotImplementedException();
		}

		public void Send(string dest, TimeoutNowRequest req)
		{
			HttpClient client;
			using (GetConnection(dest, out client))
			{
				LogStatus("timeout to " + dest, 
					client.GetAsync(string.Format("/timeoutNow?term={0}&from={1}", req.Term, req.From)));
			}
		}

		private void LogStatus(string details, Func<Task> operation)
		{
			LogStatus(details, operation());
		}

		private void LogStatus(string details, Task parentTask)
		{
			parentTask
				.ContinueWith(task =>
				{
					if (task.Exception != null)
					{
						_log.Warn("Failed to send  " + details, task.Exception);
						return;
					}
					_log.Info("Sent {0}", details);
				});
		}


		public void Dispose()
		{
			foreach (var q in _httpClientsCache.Select(x=>x.Value))
			{
				HttpClient result;
				while (q.TryDequeue(out result))
				{
					result.Dispose();
				}
			}
			_httpClientsCache.Clear();
		}


		private ReturnToQueue GetConnection(string dest, out HttpClient result)
		{
			NodeConnectionInfo info;
			if (_nodeConnectionInfos.TryGetValue(dest, out info) == false)
				throw new InvalidOperationException("Don't know how to connect to " + dest);

			var connectionQueue = _httpClientsCache.GetOrAdd(dest, _ => new ConcurrentQueue<HttpClient>());

			if (connectionQueue.TryDequeue(out result) == false)
			{
				result = new HttpClient
				{
					BaseAddress = info.Url
				};
			}

			return new ReturnToQueue(result, connectionQueue);
		}

		private struct ReturnToQueue : IDisposable
		{
			private readonly HttpClient client;
			private readonly ConcurrentQueue<HttpClient> queue;

			public ReturnToQueue(HttpClient client, ConcurrentQueue<HttpClient> queue)
			{
				this.client = client;
				this.queue = queue;
			}

			public void Dispose()
			{
				queue.Enqueue(client);
			}
		}

	}
}