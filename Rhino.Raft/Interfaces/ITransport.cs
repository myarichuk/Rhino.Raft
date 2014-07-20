using System;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Interfaces
{
	/// <summary>
	/// abstraction for transport between Raft nodes.
	/// </summary>
	public interface ITransport
	{
		void Send<T>(string sourceNode, T message);

		void Subscribe<T>(IHandler<T> messageHandler);

		void Unsubscribe<T>(IHandler<T> messageHandler);
	}
}
