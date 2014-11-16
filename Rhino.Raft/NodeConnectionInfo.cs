using System;
using Voron.Util;

namespace Rhino.Raft
{
	public class NodeConnectionInfo 
	{
		/// <summary>
		/// The name of the connection string specified in the 
		/// server configuration file. 
		/// Override all other properties of the destination
		/// </summary>

		private string url;

		public NodeConnectionInfo ConnectionInfo { Get; set; }

		public string Url
		{
			get { return url; }
			set
			{
				url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
			}
		}


		/// <summary>
		/// Node name that this will connect to
		/// </summary>
		public string NodeName { get; set; }
		
		/// <summary>
		/// The node server username to use
		/// </summary>
		public string Username { get; set; }

		/// <summary>
		/// The node server password to use
		/// </summary>
		public string Password { get; set; }

		/// <summary>
		/// The node server domain to use
		/// </summary>
		public string Domain { get; set; }

		/// <summary>
		/// The node server api key to use
		/// </summary>
		public string ApiKey { get; set; }

		/// <summary>
		/// Gets or sets if the node will ignore this destination in the client
		/// </summary>
		public bool IgnoredClient { get; set; }

		/// <summary>
		/// Gets or sets if node to this destination is disabled in both client and server.
		/// </summary>
		public bool Disabled { get; set; }

		/// <summary>
		/// Gets or sets the Client URL of the node destination
		/// </summary>
		public string ClientVisibleUrl { get; set; }


	}
}
