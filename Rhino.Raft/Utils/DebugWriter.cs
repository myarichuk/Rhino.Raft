// -----------------------------------------------------------------------
//  <copyright file="DebugTextStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;

namespace Rhino.Raft.Utils
{
	public class DebugWriter 
	{
		private readonly string _name;
		private readonly Stopwatch _duration;

		public DebugWriter(string name, Stopwatch duration)
		{
			_name = name;
			_duration = duration;
		}

		public void Write(string format, params object[] args)
		{
			Console.WriteLine("{0} @ {1,7:#,#;;0}: {2}", _name, _duration.ElapsedMilliseconds, string.Format(format, args));
		}
	}
}