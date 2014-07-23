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
				new RaftTests().AfterHeartbeatTimeout_Node_should_change_state_to_candidate();
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}
}
