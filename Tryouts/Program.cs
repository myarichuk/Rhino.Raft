using System;
using System.ComponentModel;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;
using Rhino.Raft.Tests;
using Rhino.Raft.Transport;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			var options = new StartOptions();
			options.Urls.Add("http://+:8080/");
			
			using (WebApp.Start(options, builder =>
			{
				var httpConfiguration = new HttpConfiguration();
				RaftWebApiConfig.Register(httpConfiguration);
				httpConfiguration.Properties[typeof (HttpTransportBus)] = new HttpTransportBus();
				builder.UseWebApi(httpConfiguration);
			}))
			{
				Console.ReadLine();
			}

		}
	}

}
