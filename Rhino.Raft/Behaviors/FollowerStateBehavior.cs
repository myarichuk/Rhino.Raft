// -----------------------------------------------------------------------
//  <copyright file="FollowerStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;

namespace Rhino.Raft.Behaviors
{
    public class FollowerStateBehavior : AbstractRaftStateBehavior
    {
	    public FollowerStateBehavior(RaftEngine engine) : base(engine)
	    {
		    Timeout = new Random().Next(engine.MessageTimeout/2, engine.MessageTimeout);
	    }

	    public override void HandleTimeout()
	    {
		    Engine.DebugLog.WriteLine("{0} -> Got timeout in follower mode in term {1}", Engine.Name,
			    Engine.PersistentState.CurrentTerm);
			Engine.AnnounceCandidacy();
		}
    }
}