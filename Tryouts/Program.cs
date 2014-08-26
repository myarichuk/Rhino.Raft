using System;
using Rhino.Raft.Tests;
namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			try
			{
				new RaftTests().Leader_AppendCommand_for_first_time_should_distribute_commands_between_nodes().Wait();
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}
}
