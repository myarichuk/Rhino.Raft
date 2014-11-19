using System;
using System.Web.Http;
using Rhino.Raft.Tests;
using Rhino.Raft.Transport;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			var config = new HttpConfiguration();
			RaftWebApiConfig.Register(config);

			var _httpServer = new HttpServer(config);

			Console.ReadLine();
		}
	}
}
