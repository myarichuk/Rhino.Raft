// -----------------------------------------------------------------------
//  <copyright file="DebugTextStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using NLog;

namespace Rhino.Raft.Utils
{
	public class DebugWriter 
	{
		private readonly string _name;
		private readonly Stopwatch _duration;

		private readonly Logger _log;

		public DebugWriter(string name, Stopwatch duration)
		{
			_name = name;
			_log = LogManager.GetLogger("DebugWriter." + name);
			_duration = duration;
		}

		public void Write(string format, object arg1)
		{
			Write(format, new []{arg1});
		}

		public void Write(string format, object arg1, object arg2)
		{
			Write(format, new[] { arg1, arg2 });
		}

		public void Write(string format, params object[] args)
		{
			_log.Debug("{0} @ {1,7:#,#;;0}: {2}", _name, _duration.ElapsedMilliseconds, string.Format(format, args));
		}
	}
}