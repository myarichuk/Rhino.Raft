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
					for(int j = 2; j < 5; j++)
						test.Network_partition_for_less_time_than_timeout_can_be_healed_without_elections(j);
				}
				Console.WriteLine("--------------------------------------------------------------------------");
			}
		}
	}
}
