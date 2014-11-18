using System.Linq;
using Newtonsoft.Json;

namespace Rhino.Raft.Messages
{
	public class AppendEntriesRequest : BaseMessage
	{
		public long Term { get; set; }
		public string LeaderId { get; set; }
		public long PrevLogIndex { get; set; }
		public long PrevLogTerm { get; set; }
		[JsonIgnore]
		public LogEntry[] Entries { get; set; }
		public int EntriesCount { get { return Entries == null ? 0 : Entries.Length; } }
		public long LeaderCommit { get; set; }
	}
}