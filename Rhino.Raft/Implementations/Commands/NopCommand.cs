using System.Threading.Tasks;
using Rhino.Raft.Interfaces;

namespace Rhino.Raft.Implementations.Commands
{
	public class NopCommand : ICommand
	{
		public long AssignedIndex { get; set; }
		public TaskCompletionSource<object> Completion { get; set; }
		public bool BufferCommand { get; set; }
	}
}
