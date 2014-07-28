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
		    var random = new Random(Engine.Name.GetHashCode());
		    Timeout = random.Next(engine.MessageTimeout/2, engine.MessageTimeout);
	    }

	    public override void HandleTimeout()
	    {
		    Engine.DebugLog.Write("Got timeout in follower mode in term {0}", Engine.PersistentState.CurrentTerm);
			Engine.AnnounceCandidacy();
		}
    }
}