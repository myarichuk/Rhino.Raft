using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Rhino.Raft.Commands
{
	public abstract class Command
	{
		public long AssignedIndex { get; set; }

		[JsonIgnore]
		public TaskCompletionSource<object> Completion { get; set; }

		public bool BufferCommand { get; set; }
	}
}
