using Rhino.Raft.Commands;

namespace Rhino.Raft
{
	public interface ICommandSerializer
	{
		byte[] Serialize(Command cmd);
		Command Deserialize(byte[] cmd);
	}
}
