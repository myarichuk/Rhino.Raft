// -----------------------------------------------------------------------
//  <copyright file="AbstractRaftStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Rhino.Raft;
using Rhino.Raft.Behaviors;

namespace Consensus.Raft.Behaviors
{
    public class LeaderStateBehavior : AbstractRaftStateBehavior
    {
	    public LeaderStateBehavior(RaftEngine engine) : base(engine)
	    {
	    }
    }
}