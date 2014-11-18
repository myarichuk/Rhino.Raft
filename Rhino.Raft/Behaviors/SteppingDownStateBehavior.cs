// -----------------------------------------------------------------------
//  <copyright file="SteppingDownStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class SteppingDownStateBehavior : LeaderStateBehavior
	{
		private DateTime _stepDownStart;

		public SteppingDownStateBehavior(RaftEngine engine) : base(engine)
		{
			_stepDownStart = DateTime.UtcNow;
		}

		public override RaftEngineState State
		{
			get { return RaftEngineState.SteppingDown; }
		}

		public override void Handle(string destination, AppendEntriesResponse resp)
		{
			base.Handle(destination, resp);

			var maxIndexOnQuorom = GetMaxIndexOnQuorom();

			var lastLogEntry = Engine.PersistentState.LastLogEntry();

			if (maxIndexOnQuorom >= lastLogEntry.Index)
			{
				var bestMatch = _matchIndexes.OrderByDescending(x => x.Value).Select(x => x.Key).FirstOrDefault();

				Engine.Transport.Send(bestMatch, new TimeoutNowRequest
				{
					Term = Engine.PersistentState.CurrentTerm,
					From = Engine.Name
				});

				Engine.SetState(RaftEngineState.None);
			}
		}
	}
}