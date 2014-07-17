using System.Threading.Tasks;

namespace Rhino.Raft.Interfaces
{
	public interface ICommand
	{
		long AssignedIndex { get; set; }

		TaskCompletionSource<object> Completion { get; set; }

		bool BufferCommand { get; set; }
	}
}
