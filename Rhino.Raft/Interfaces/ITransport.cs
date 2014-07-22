using System;

namespace Rhino.Raft.Interfaces
{

	/// <summary>
	/// abstraction for transport between Raft nodes.
	/// </summary>
	public interface ITransport
	{
		TimeSpan HeartbeatTimeout { get; set; }

		void Send<T>(string dest, T message);
		
		void Send<T>(T message);

		void RegisterHandler<T>(IHandler<T> messageHandler);

		void Unregister<T>(IHandler<T> messageHandler);
	}

}