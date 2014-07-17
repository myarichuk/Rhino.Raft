namespace Rhino.Raft.Interfaces
{
	public interface ISerializer
	{
		byte[] Serialize(ICommand command);

		ICommand Deserialize(byte[] cmd);
	}
}
