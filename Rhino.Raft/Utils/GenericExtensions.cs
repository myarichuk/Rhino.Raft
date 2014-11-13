using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace Rhino.Raft
{
	public static class GenericExtensions
	{
		//good idea - adapted version of code from http://stackoverflow.com/questions/17635440/how-to-wait-for-a-single-event-in-c-with-timeout-and-cancellation
		//this is kind of preparation of airconditioning before its actually needed, but in the tests it is very useful
		public static Task WaitForEventTask<TObject>(this TObject objectWithEvents, Action<TObject,Action> subscribeToEvent, CancellationToken? cancellationToken = null)
			where TObject : class 
		{
			var tcs = new TaskCompletionSource<object>();
			Action handler = null;

			CancellationTokenRegistration? registration = null;
			if (cancellationToken.HasValue)
			{
				registration = cancellationToken.Value.Register(() =>
				{
					tcs.TrySetCanceled();
				});
			}
			handler = () =>
			{
				if (registration.HasValue)
					registration.Value.Dispose();
				tcs.TrySetResult(null);
			};

			subscribeToEvent(objectWithEvents,handler);
			return tcs.Task;
		}
	}
}
