// -----------------------------------------------------------------------
//  <copyright file="FollowerStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
    public class FollowerStateBehavior : AbstractRaftStateBehavior
    {
	    public FollowerStateBehavior(RaftEngine engine) : base(engine)
        {
        }

	    public void Handle(string source, RequestVoteRequest req)
	    {
		    
	    }

	    public void Timeout()
	    {
		    Engine.AnnounceCandidacy();
	    }

	    public override void RunOnce()
		{
			
		}
	}
}