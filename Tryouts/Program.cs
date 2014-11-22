using System;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;
using Rachis;
using Rachis.Tests;
using Rachis.Transport;
using TailFeather.Client;
using Voron;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			var tailFeatherClient = new TailFeatherClient(new Uri("http://localhost:9077"));

			Console.WriteLine(tailFeatherClient.Get("ravendb").Result.ToString());
		}
	}

}
