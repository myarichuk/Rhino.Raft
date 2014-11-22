using System;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Newtonsoft.Json;
using Owin;
using Rhino.Raft;
using Rhino.Raft.Storage;
using Rhino.Raft.Tests;
using Rhino.Raft.Transport;
using Voron;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 10; i++)
			{
				Console.WriteLine(i);
				using (var s = new TopologyChangesTests())
				{
					s.Non_leader_Node_removed_from_cluster_should_update_peers_list(5);
				}
			}
		}
	}

}
