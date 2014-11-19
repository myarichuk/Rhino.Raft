using Rhino.Raft.Storage;

namespace Rhino.Raft.Commands
{
	public class TopologyChangeCommand : Command
	{
		public Topology Requested { get; set; }
		public Topology Previous { get; set; }
	}
}