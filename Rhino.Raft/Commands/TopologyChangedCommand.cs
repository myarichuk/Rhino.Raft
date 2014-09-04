using Rhino.Raft.Storage;

namespace Rhino.Raft.Commands
{
	public class TopologyChangedCommand : Command
	{
		public Topology Existing { get; set; }
		public Topology Requested { get; set; }
	}
}