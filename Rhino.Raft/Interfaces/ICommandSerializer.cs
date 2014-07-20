namespace Rhino.Raft.Interfaces
{
	public interface ICommandSerializer
	{
		byte[] Serialize(ICommand command);

		ICommand Deserialize(byte[] cmd);
	}
}
