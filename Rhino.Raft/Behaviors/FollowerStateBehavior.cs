// -----------------------------------------------------------------------
//  <copyright file="FollowerStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;

namespace Rhino.Raft.Behaviors
{
    public class FollowerStateBehavior : AbstractRaftStateBehavior
    {
	    public FollowerStateBehavior(RaftEngine engine) : base(engine)
	    {
		    HeartbeatTimeout += HandleHeartbeatTimeout;
	    }


	    public void HandleHeartbeatTimeout()
	    {
		    Engine.AnnounceCandidacy();
	    }

	    public override void RunOnce()
		{
			//nothing to do here
			Thread.Sleep(100);
		}
	}
}