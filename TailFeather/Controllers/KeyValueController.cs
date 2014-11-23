using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TailFeather.Storage;

namespace TailFeather.Controllers
{
public class KeyValueController : TailFeatherController
{
	[HttpGet]
	[Route("tailfeather/key-val/read")]
	public HttpResponseMessage Read([FromUri] string key)
	{
		var read = StateMachine.Read(key);
		if (read == null)
		{
			return Request.CreateResponse(HttpStatusCode.NotFound, new
			{
				RaftEngine.State,
				Key = key,
				Missing = true
			});
		}
		return Request.CreateResponse(HttpStatusCode.OK, new
		{
			RaftEngine.State,
			Key = key,
			Value = read
		});
	}

	[HttpGet]
	[Route("tailfeather/key-val/set")]
	public Task<HttpResponseMessage> Set([FromUri] string key, [FromUri] string val)
	{
		JToken jVal;
		try
		{
			jVal = JToken.Parse(val);
		}
		catch (JsonReaderException)
		{
			jVal = val;
		}

		var op = new KeyValueOperation
		{
			Key = key,
			Type = KeyValueOperationTypes.Add,
			Value = jVal
		};

		return Batch(new[] { op });
	}

	[HttpGet]
	[Route("tailfeather/key-val/del")]
	public Task<HttpResponseMessage> Del([FromUri] string key)
	{
		var op = new KeyValueOperation
		{
			Key = key,
			Type = KeyValueOperationTypes.Del,
		};

		return Batch(new[] { op });
	}

	[HttpPost]
	[Route("tailfeather/key-val/batch")]
	public async Task<HttpResponseMessage> Batch()
	{
		var stream = await Request.Content.ReadAsStreamAsync();
		var operations = new JsonSerializer().Deserialize<KeyValueOperation[]>(new JsonTextReader(new StreamReader(stream)));

		return await Batch(operations);
	}

	private async Task<HttpResponseMessage> Batch(KeyValueOperation[] operations)
	{
		var taskCompletionSource = new TaskCompletionSource<object>();
		RaftEngine.AppendCommand(new OperationBatchCommand
		{
			Batch = operations,
			Completion = taskCompletionSource
		});
		await taskCompletionSource.Task;

		return Request.CreateResponse(HttpStatusCode.Accepted);
	}
}
}