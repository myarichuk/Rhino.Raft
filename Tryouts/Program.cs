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
					test.Leader_AppendCommand_for_first_time_should_distribute_commands_between_nodes(4).Wait();
				}
				Console.WriteLine("--------------------------------------------------------------------------");
			}
		}
	}
}
