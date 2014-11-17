using Xunit;
using Xunit.Extensions;

namespace Rhino.Raft.Tests
{
	public class BasicTests : RaftTestsBase
	{
		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(5)]
		[InlineData(7)]
		public void CanApplyCommitAcrossAllCluster(int amount)
		{
			var leader = CreateNetworkAndWaitForLeader(amount);
			var commits = WaitForCommitsOnCluster<DictionaryCommand.Set>(5);
			for (int i = 0; i < 5; i++)
			{
				leader.AppendCommand(new DictionaryCommand.Set
				{
					Key = i.ToString(),
					Value = i
				});
			}
			commits.Wait();

			foreach (var node in Nodes)
			{
				for (int i = 0; i < 5; i++)
				{
					var dictionary = ((DictionaryStateMachine)node.StateMachine).Data;
					Assert.Equal(i, dictionary[i.ToString()]);
				}
			}
		}
	}
}