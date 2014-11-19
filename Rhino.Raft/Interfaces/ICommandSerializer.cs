using Rhino.Raft.Commands;

namespace Rhino.Raft.Interfaces
{
	public interface ICommandSerializer
	{
		byte[] Serialize(Command cmd);
		Command Deserialize(byte[] cmd);
	}
}
