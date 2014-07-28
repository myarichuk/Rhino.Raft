// -----------------------------------------------------------------------
//  <copyright file="CandidateStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class CandidateStateBehavior : AbstractRaftStateBehavior
    {
		private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
		private int _votesForMyLeadership;
		private readonly Random _random;
		private int _currentTimeout;
		public int VotesForMyLeadership
		{
			get { return _votesForMyLeadership; }
			set { _votesForMyLeadership = value; }
		}

		public CandidateStateBehavior(RaftEngine engine) : base(engine)
		{
			_random = new Random(engine.Name.GetHashCode());
			_currentTimeout = Timeout = _random.Next(engine.MessageTimeout / 2, engine.MessageTimeout);
			VoteForSelf();
	    }

		private void VoteForSelf()
		{
			Engine.DebugLog.WriteLine("{0} -> Voting for myself in term {1}", Engine.Name, Engine.PersistentState.CurrentTerm);
			Handle(Engine.Name,
				new RequestVoteResponse
				{
					Term = Engine.PersistentState.CurrentTerm,
					VoteGranted = true,
					Message = String.Format("{0} -> Voting for myself", Engine.Name)
				});
		}

		public override void HandleTimeout()
	    {
			Engine.DebugLog.WriteLine("{1} -> Timeout ({2:#,#;;0} ms) for elections in term {0}", Engine.PersistentState.CurrentTerm,
				  Engine.Name, _currentTimeout);

			_currentTimeout = Timeout = _random.Next(Engine.MessageTimeout / 2, Engine.MessageTimeout); 
			VotesForMyLeadership = 0;
			Engine.AnnounceCandidacy();
			VoteForSelf();
	    }	

		public override void Handle(string source,RequestVoteResponse resp)
		{
			Engine.DebugLog.WriteLine("{2} -> Received RequestVoteResponse, Term = {0}, VoteGranted = {1}, Message = {3}", resp.Term,resp.VoteGranted,Engine.Name,resp.Message);

			if (resp.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(resp.Term);
				return;
			}

			Timeout -= (int)_stopwatch.ElapsedMilliseconds;
			if (Timeout <= 0)
			{
				Timeout = 0;
				return;
			}

			_stopwatch.Restart();

			if (!resp.VoteGranted)
			{
				return;
			}

			Interlocked.Increment(ref _votesForMyLeadership);
			if (_votesForMyLeadership < Engine.QuorumSize)
			{
				Engine.DebugLog.WriteLine("{0} -> Not enough votes for leadership, vote count = {1}",Engine.Name,_votesForMyLeadership);
				return;
			}

			Engine.DebugLog.WriteLine("{0} -> selected as leader, term = {1}", Engine.Name, resp.Term);
			Engine.SetState(RaftEngineState.Leader);
		}

    }
}