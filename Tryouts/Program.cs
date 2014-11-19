using System;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;
using Rhino.Raft;
using Rhino.Raft.Tests;
using Rhino.Raft.Transport;
using Voron;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			var httpTransport = new HttpTransport("me");
			httpTransport.Register(new NodeConnectionInfo
			{
				Name = "oren",
				Url = new Uri("http://localhost:8080")
			});

			var raftEngineOptions = new RaftEngineOptions("me", StorageEnvironmentOptions.CreateMemoryOnly(),httpTransport, new DictionaryStateMachine())
			{
				MessageTimeout = 60*1000,
				AllVotingNodes = new[] { "me", "oren"}
			};
			var raftEngine = new RaftEngine(raftEngineOptions);

			using (WebApp.Start(new StartOptions
			{
				Urls = { "http://+:9079/"}
			}, builder =>
			{
				var httpConfiguration = new HttpConfiguration();
				RaftWebApiConfig.Register(httpConfiguration);
				httpConfiguration.Properties[typeof (HttpTransportBus)] = httpTransport.Bus;
				builder.UseWebApi(httpConfiguration);
			}))
			{
				Console.ReadLine();
			}

		}
	}

}
