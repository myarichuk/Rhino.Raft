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
				Console.WriteLine("--------------------------------------------------------------------------");
				Console.WriteLine(i);
				using (var test = new RaftTests())
				{
					for(int j = 2; j < 4; j++)
						test.Node_added_to_cluster_should_update_peers_list(j);
				}
				Console.WriteLine("--------------------------------------------------------------------------");
			}
		}
	}
}
