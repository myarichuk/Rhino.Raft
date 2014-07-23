// -----------------------------------------------------------------------
//  <copyright file="FollowerStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

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
			Engine.AnnounceCandidacy();
		}
    }
}