namespace Rhino.Raft.Messages
{
    public class MessageEnvelope
    {
        public string Destination { get; set; }
        public object Message { get; set; }
    }
}