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
			var deserializeObject = JsonConvert.DeserializeObject<Topology>(
				"{'AllVotingNodes':['node1'],'NonVotingNodes':[],'PromotableNodes':[],'QuorumSize':1,'AllNodes':['node1'],'HasVoters':true}",
				new JsonSerializerSettings
				{
					ObjectCreationHandling = ObjectCreationHandling.Auto
				});

			Console.WriteLine(deserializeObject);
		}
	}

}
