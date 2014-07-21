namespace Rhino.Raft
{
	public interface IHandler<in T>
	{
		string Name { get; }

		void Handle(string source,T @message);
	}
}
