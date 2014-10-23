using Rhino.Raft.Storage;

namespace Rhino.Raft.Commands
{
	public class TopologyChangeCommand : Command
	{
		public Topology Existing { get; set; }
		public Topology Requested { get; set; }

	}
}