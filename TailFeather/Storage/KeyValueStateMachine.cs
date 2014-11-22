using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;
using Voron;
using Voron.Util.Conversion;

namespace TailFeather
{
public class KeyValueStateMachine : IRaftStateMachine
{
	readonly StorageEnvironment _storageEnvironment;

	public KeyValueStateMachine(StorageEnvironmentOptions options)
	{
		_storageEnvironment = new StorageEnvironment(options);
		using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
		{
			_storageEnvironment.CreateTree(tx, "items");
			var metadata = _storageEnvironment.CreateTree(tx, "$metadata");
			var readResult = metadata.Read("last-index");
			if (readResult != null)
				LastAppliedIndex = readResult.Reader.ReadLittleEndianInt64();
			tx.Commit();
		}
	}

	public event EventHandler<KeyValueOperation> OperatonExecuted;

	protected void OnOperatonExecuted(KeyValueOperation e)
	{
		var handler = OperatonExecuted;
		if (handler != null) handler(this, e);
	}

	public JToken Read(string key)
	{
		using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
		{
			var items = tx.ReadTree("items");

			var readResult = items.Read(key);

			if (readResult == null)
				return null;


			return JToken.ReadFrom(new JsonTextReader(new StreamReader(readResult.Reader.AsStream())));
		}
	}

	public long LastAppliedIndex { get; private set; }

	public void Apply(LogEntry entry, Command cmd)
	{
		var batch = (OperationBatchCommand)cmd;
		Apply(batch.Batch, cmd.AssignedIndex);
	}

	public bool SupportSnapshots { get { return false;  }}

	public void CreateSnapshot(long index, long term)
	{
		throw new NotImplementedException();
	}

	public ISnapshotWriter GetSnapshotWriter()
	{
		throw new NotImplementedException();
	}

	public void ApplySnapshot(long term, long index, Stream stream)
	{
		throw new NotImplementedException();
	}

	private void Apply(IEnumerable<KeyValueOperation> ops, long commandIndex)
	{
		using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
		{
			var items = tx.ReadTree("items");
			var metadata = tx.ReadTree("$metadata");
			metadata.Add("last-index", EndianBitConverter.Little.GetBytes(commandIndex));
			var ms = new MemoryStream();
			foreach (var op in ops)
			{
				switch (op.Type)
				{
					case KeyValueOperationTypes.Add:
						ms.SetLength(0);

						var streamWriter = new StreamWriter(ms);
						op.Value.WriteTo(new JsonTextWriter(streamWriter));
						streamWriter.Flush();

						ms.Position = 0;
						items.Add(op.Key, ms);
						break;
					case KeyValueOperationTypes.Del:
						items.Delete(op.Key);
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
				OnOperatonExecuted(op);
			}

			tx.Commit();
		}
	}


	public void Dispose()
	{
		if (_storageEnvironment != null)
			_storageEnvironment.Dispose();
	}
}
}