// -----------------------------------------------------------------------
//  <copyright file="AbstractRaftStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
    public class LeaderStateBehavior : AbstractRaftStateBehavior, IHandler<Command>
    {
		private readonly Dictionary<string, long> _matchIndexes = new Dictionary<string, long>();
		private readonly Dictionary<string, long> _nextIndexes = new Dictionary<string, long>();
		
		private readonly Stopwatch _timer;
	    private bool _bufferCommand;

		public LeaderStateBehavior(RaftEngine engine)
			: base(engine)
		{
			Engine.Transport.RegisterHandler<Command>(this);

			_timer = Stopwatch.StartNew();
			var lastLogEntry = Engine.PersistentState.LastLogEntry() ?? new LogEntry();

			foreach (var peer in Engine.AllPeers)
			{
				_nextIndexes[peer] = lastLogEntry.Index + 1;
				_matchIndexes[peer] = 0;
			}

			Engine.FireAndForgetCommand(new NopCommand());
			_bufferCommand = true;
		}

	    public override void RunOnce()
	    {
			var remaining = Engine.HeartbeatTimeout - _timer.Elapsed;
			if (_bufferCommand == false || remaining <= TimeSpan.Zero)
			{
				_bufferCommand = true;
				_timer.Restart();

				foreach (var peer in Engine.AllPeers)
				{
					SendEntriesToPeer(peer);
				}
			}

			Thread.Sleep(remaining);
		}


		private void SendEntriesToPeer(string peer)
		{
			var nextIndex = _nextIndexes[peer];
			var entries = Engine.PersistentState.LogEntriesAfter(nextIndex)
												.Take(Engine.MaxEntriesPerRequest)
												.ToArray();

			var prevLogEntry = entries.Length == 0
				? Engine.PersistentState.LastLogEntry()
				: Engine.PersistentState.GetLogEntry(entries[0].Index - 1);

			var aer = new AppendEntriesRequest
			{
				Entries = entries,
				LeaderCommit = Engine.CommitIndex,
				LeaderId = Engine.Name,
				PrevLogIndex = prevLogEntry.Index,
				PrevLogTerm = prevLogEntry.Term,
				Term = Engine.PersistentState.CurrentTerm
			};

			Engine.Transport.Send(peer, aer);
		}

		protected RaftEngineState State
		{
			get { return RaftEngineState.Leader; }
		}

		public override void Handle(string source, AppendEntriesResponse resp)
		{
			// there is a new leader in town, time to step down
			if (resp.CurrentTerm > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(resp.CurrentTerm);
				return;
			}
			if (resp.Success == false)
			{
				// go back in the log, this peer isn't matching us at this location
				_nextIndexes[source] = _nextIndexes[source] - 1;
			}
		}

		public void Handle(string source, Command command)
	    {
		    if (command != null)
		    {
			    _bufferCommand &= command.BufferCommand;
			    var commandEntry = Engine.CommandSerializer.Serialize(command);
			    command.AssignedIndex = Engine.PersistentState.AppendToLeaderLog(commandEntry);
		    }
	    }
    }
}