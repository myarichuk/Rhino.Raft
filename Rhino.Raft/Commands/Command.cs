using System.Threading.Tasks;

namespace Rhino.Raft.Commands
{
	public abstract class Command
	{
		public long AssignedIndex { get; set; }
		public TaskCompletionSource<object> Completion { get; set; }
		public bool BufferCommand { get; set; }
	}
}
