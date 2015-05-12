using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using Voron;

namespace TimeSeries
{
	public class Program
	{
		private static void Main(string[] args)
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("R2");
			//storageEnvironmentOptions.ManualFlushing = true;
			using (var tss = new TimeSeriesStorage(storageEnvironmentOptions))
			{
				Console.WriteLine("running");
				var sp = Stopwatch.StartNew();
				ImportWikipedia(tss);
				sp.Stop();
				Console.WriteLine(sp.Elapsed);

				/*using (var r = tss.CreateReader())
				{
					foreach (var point in r.Query("1234", DateTime.Now.AddDays(-1), DateTime.Now.AddMinutes(5)))
					{
						Console.WriteLine(point.At + " " + point.Value);
					}
				}*/
			}
		}

		private static void ImportWikipedia(TimeSeriesStorage tss)
		{
			var dir = @"E:\TimeSeries\20150401\Compressed";
			var path = Path.Combine(dir, "pagecounts-20150401-000000.gz");

			using (var stream = File.OpenRead(path))
			using (var uncompressed = new GZipStream(stream, CompressionMode.Decompress))
			{
				int lines = 0;
				var writer = tss.CreateWriter();
				try
				{
					using (var reader = new StreamReader(uncompressed))
					{
						string line;
						while ((line = reader.ReadLine()) != null)
						{
							if (string.IsNullOrEmpty(line))
								continue;

							var items = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

							if (items.Length < 4)
								continue;

							var entryName = items[0] + "/" + WebUtility.UrlDecode(items[1]);
							if (entryName.Length > 512)
								continue;

							var time = DateTime.ParseExact("20150401-000000", "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);

							writer.Append("views/" + entryName, time, int.Parse(items[2]));
							writer.Append("sizes/" + entryName, time, double.Parse(items[3]));

							if (lines++%1000 == 0)
							{
								writer.Commit();
								writer.Dispose();
								writer = tss.CreateWriter();
							}

						}
					}
				}
				finally
				{
					writer.Dispose();
				}
			}
		}
	}
}