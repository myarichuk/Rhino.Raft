namespace Rhino.Raft.Interfaces
{
	public interface IHandler<in T>
	{
		string Name { get; }

		void Handle(string source,T @message);
	}
}
