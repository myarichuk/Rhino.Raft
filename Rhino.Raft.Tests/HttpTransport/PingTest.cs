// -----------------------------------------------------------------------
//  <copyright file="PingTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Web.Http;
using Rhino.Raft.Transport;

namespace Rhino.Raft.Tests.HttpTransport
{
	public class PingTest : IDisposable
	{
		private HttpServer _httpServer;

		public PingTest()
		{
			var config = new HttpConfiguration();
			RaftWebApiConfig.Register(config);

			_httpServer = new HttpServer(config);
		}

		public void Dispose()
		{
			_httpServer.Dispose();
		}
	}
}