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
			var wtf = @"C:\Users\Ayende\Documents\Fiddler2\Captures\51_.txt";

			var jArray = JArray.Parse(File.ReadAllText(wtf));
			int i = 0;
			foreach (var item in jArray)
			{
				i++;
				File.WriteAllText(i+".json", item.ToString(Formatting.Indented));
			}
		}
	}

}
