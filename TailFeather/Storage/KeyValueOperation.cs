using Newtonsoft.Json.Linq;

namespace TailFeather
{
public class KeyValueOperation
{
	public KeyValueOperationTypes Type;
	public string Key;
	public JToken Value;
}
}