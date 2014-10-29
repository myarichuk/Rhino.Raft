using System;
using Rhino.Raft.Tests;
namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				using (var test = new RaftTests())
				{
					test.Cluster_nodes_are_able_to_recover_after_shutdown_in_the_middle_of_topology_change();
				}
			}
		}
	}
}
