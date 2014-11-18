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
	    private bool _avoidLeadership;
	    private readonly long _currentTermWhenWeBecameFollowers;

	    public FollowerStateBehavior(RaftEngine engine, bool avoidLeadership) : base(engine)
	    {
		    _avoidLeadership = avoidLeadership;
		    _currentTermWhenWeBecameFollowers = engine.PersistentState.CurrentTerm + 1;// we are going to have a new term immediately.
		    var random = new Random(Engine.Name.GetHashCode() ^ (int)DateTime.Now.Ticks);
			Timeout = random.Next(engine.Options.MessageTimeout / 2, engine.Options.MessageTimeout);
	    }

	    public override RaftEngineState State
	    {
		    get { return RaftEngineState.Follower; }
	    }

	    public override void HandleTimeout()
	    {
			if (_avoidLeadership && _currentTermWhenWeBecameFollowers >= Engine.PersistentState.CurrentTerm)
		    {
				Engine.DebugLog.Write("Got timeout in follower mode in term {0}, but we are in avoid leadership mode following a step down, so we'll let this one slide. Next time, I'm going to be the leader again!", 
					Engine.PersistentState.CurrentTerm);
				LastHeartbeatTime = DateTime.UtcNow;
			    _avoidLeadership = false;
			    return;
		    }
		    Engine.DebugLog.Write("Got timeout in follower mode in term {0}", Engine.PersistentState.CurrentTerm);
			Engine.SetState(RaftEngineState.Candidate);
		}
    }
}