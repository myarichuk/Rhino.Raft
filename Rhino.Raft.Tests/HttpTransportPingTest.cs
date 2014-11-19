// -----------------------------------------------------------------------
//  <copyright file="HttpTransportPingTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;
using Rhino.Raft.Messages;
using Rhino.Raft.Transport;
using Voron;
using Xunit;

namespace Rhino.Raft.Tests
{
	public class HttpTransportPingTest : IDisposable
	{
		private readonly IDisposable _server;
		private readonly RaftEngine _raftEngine;

		public HttpTransportPingTest()
		{
			var node1Transport = new HttpTransport("node1");
			node1Transport.Register(new NodeConnectionInfo { Name = "node1", Url = new Uri("http://localhost:9079") });
			node1Transport.Register(new NodeConnectionInfo { Name = "node2", Url = new Uri("http://localhost:9078") });
			node1Transport.Register(new NodeConnectionInfo { Name = "node3", Url = new Uri("http://localhost:9077") });

			var engineOptions = new RaftEngineOptions("node1", StorageEnvironmentOptions.CreateMemoryOnly(), node1Transport, new DictionaryStateMachine())
			{
				AllVotingNodes = new[] { "node1", "node2", "node3" },
				MessageTimeout = 60*1000
			};
			_raftEngine = new RaftEngine(engineOptions);

			_server = WebApp.Start(new StartOptions
			{
				Urls = { "http://+:9079/" }
			}, builder =>
			{
				var httpConfiguration = new HttpConfiguration();
				RaftWebApiConfig.Register(httpConfiguration);
				httpConfiguration.Properties[typeof(HttpTransportBus)] = node1Transport.Bus;
				builder.UseWebApi(httpConfiguration);
			});
		}

		[Fact]
		public void CanSendRequestVotesAndGetReply()
		{
			var node2Transport = new Transport.HttpTransport("node2");
			node2Transport.Register(new NodeConnectionInfo
			{
				Name = "node1",
				Url = new Uri("http://localhost:9079")
			});

			node2Transport.Send("node1", new RequestVoteRequest
			{
				TrialOnly = true,
				From = "node2",
				Term = 3,
				LastLogIndex = 2,
				LastLogTerm = 2,
			});

			MessageContext context;
			var gotIt = node2Transport.TryReceiveMessage(2500, CancellationToken.None, out context);

			Assert.True(gotIt);

			Assert.True(context.Message is RequestVoteResponse);
		}


		[Fact]
		public void CanSendEntries()
		{
			var node2Transport = new Transport.HttpTransport("node2");
			node2Transport.Register(new NodeConnectionInfo
			{
				Name = "node1",
				Url = new Uri("http://localhost:9079")
			});

			node2Transport.Send("node1", new AppendEntriesRequest
			{
				From = "node2"
			});
			

			MessageContext context;
			var gotIt = node2Transport.TryReceiveMessage(2500, CancellationToken.None, out context);

			Assert.True(gotIt);

			Assert.True(context.Message is RequestVoteResponse);
		}

		public void Dispose()
		{
			_server.Dispose();
			_raftEngine.Dispose();

		}
	}
}