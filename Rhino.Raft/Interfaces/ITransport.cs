namespace Rhino.Raft.Interfaces
{
	/// <summary>
	/// abstraction for transport between Raft nodes.
	/// </summary>
	public interface ITransport
	{
		void Send<T>(string sourceNode, T message);
	}
}
