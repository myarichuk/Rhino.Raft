using Rachis.Commands;

namespace TailFeather
{
	public class OperationBatchCommand : Command
	{
		public KeyValueOperation[] Batch { get; set; }
	}
}