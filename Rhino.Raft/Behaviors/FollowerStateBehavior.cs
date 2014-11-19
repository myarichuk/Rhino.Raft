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
			LastHeartbeatTime = DateTime.UtcNow;

		    if (Engine.PersistentState.IsLeaderPotential == false)
		    {
			    _log.Info("Not a leader material, can't become a candidate. (This will change the first time we'll get a append entries request.");
			    return;
		    }

			if (_avoidLeadership && _currentTermWhenWeBecameFollowers >= Engine.PersistentState.CurrentTerm)
			{
				_log.Info("Got timeout in follower mode in term {0}, but we are in avoid leadership mode following a step down, so we'll let this one slide. Next time, I'm going to be the leader again!", 
					Engine.PersistentState.CurrentTerm);
			    _avoidLeadership = false;
			    return;
		    }
			_log.Info("Got timeout in follower mode in term {0}", Engine.PersistentState.CurrentTerm);
			Engine.SetState(RaftEngineState.Candidate);
		}
    }
}