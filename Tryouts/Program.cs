using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TailFeather.Client;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			var tailFeatherClient = new TailFeatherClient(new Uri("http://localhost:9078"));

			int i =0;
			while (true)
			{
				tailFeatherClient.Set("now-"+i, DateTime.Now);
				Console.WriteLine(i++);
				Console.ReadKey();
			}
		}
	}

}
