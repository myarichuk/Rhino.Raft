using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TailFeather.Client;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			var tailFeatherClient = new TailFeatherClient(new Uri("http://localhost:9077"));

			var tasks = new List<Task>();
			for (int i = 0; i < 50*1000; i++)
			{
				tasks.Add(tailFeatherClient.Set("users/" + i, false));
				if (tasks.Count > 50)
				{
					Task.WaitAll(tasks.ToArray());

					tasks.Clear();
				}
			}

		}
	}

}
