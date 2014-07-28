using System;
using Rhino.Raft.Tests;
namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				try
				{
					new RaftTests().On_many_node_network_can_be_only_one_leader(3);
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					break;
				}
			}
		}
	}
}
