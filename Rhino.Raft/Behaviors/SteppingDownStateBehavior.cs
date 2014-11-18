// -----------------------------------------------------------------------
//  <copyright file="SteppingDownStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class SteppingDownStateBehavior : LeaderStateBehavior
	{
		private readonly Stopwatch _stepdownDuration;

		public SteppingDownStateBehavior(RaftEngine engine) : base(engine)
		{
			_stepdownDuration = Stopwatch.StartNew();
			// we are sending this to ourselves because we want to make 
			// sure that we immediately check if we can step down
			Engine.Transport.Send(Engine.Name, new AppendEntriesResponse
			{
				CurrentTerm = Engine.PersistentState.CurrentTerm,
				From = Engine.Name,
				LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
				LeaderId = Engine.Name,
				Message = null,
				Success = true
			});
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
				Engine.DebugLog.Write("Done sending all events to the cluster, can step down gracefully now");
				TransferToBestMatch();
			}
		}

		private void TransferToBestMatch()
		{
			var bestMatch = _matchIndexes.OrderByDescending(x => x.Value).Select(x => x.Key).FirstOrDefault();

			Engine.Transport.Send(bestMatch, new TimeoutNowRequest
			{
				Term = Engine.PersistentState.CurrentTerm,
				From = Engine.Name
			});
			Engine.DebugLog.Write("Transfering cluster leadership to {0}", bestMatch);
			Engine.SetState(RaftEngineState.Follower);
		}

		public override void HandleTimeout()
		{
			base.HandleTimeout();
			if (_stepdownDuration.Elapsed > Engine.Options.MaxStepDownDrainTime)
			{
				Engine.DebugLog.Write("Step down has aborted after {0} because this is greater than the max step down time", _stepdownDuration.Elapsed);
				TransferToBestMatch();
			}
		}
	}
}