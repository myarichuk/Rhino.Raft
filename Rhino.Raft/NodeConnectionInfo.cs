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

		public short Port { get; set; }

		public NodeConnectionInfo()
		{
			Port = Default.HttpTransportListeningPort;
		}

		public Uri GetUriForMessageSending<TMessage>()
			where TMessage : class 
		{
			return new Uri(String.Format("http://{0}:{1}/{2}",  Url, Port, typeof(TMessage).Name));
		}
	}
}
