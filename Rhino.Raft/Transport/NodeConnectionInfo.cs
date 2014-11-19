// -----------------------------------------------------------------------
//  <copyright file="NodeConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Rhino.Raft.Transport
{
	public class NodeConnectionInfo
	{
		public Uri Url { get; set; }

		public string Name { get; set; }

		public string Username { get; set; }

		public string Domain { get; set; }

		public string ApiKey { get; set; }
	}
}