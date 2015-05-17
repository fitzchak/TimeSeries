using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using Voron;
using Voron.Debugging;
using Voron.Impl;
using Voron.Trees;

namespace TimeSeries
{
	public class Program
	{
		private static void Main(string[] args)
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("R201");
			//storageEnvironmentOptions.ManualFlushing = true;
			using (var tss = new TimeSeriesStorage(storageEnvironmentOptions))
			{
				Console.WriteLine("running");
				var sp = Stopwatch.StartNew();
				var addedData = new List<string>();
				ImportWikipedia(tss, addedData);
				sp.Stop();
				Console.WriteLine(sp.Elapsed);

				File.WriteAllLines("AddedData.txt", addedData);

				/*using (var tx = tss.StorageEnvironment.NewTransaction(TransactionFlags.Read))
				{
					var readTree = tx.ReadTree("data");

					var stats = new Stats[10];
					//using (var fileStream = File.Create("DataContent.csv"))
					// using (var writer = new StreamWriter(fileStream, Encoding.UTF8))
					{
						writer.WriteLine(string.Join(",", new[]
						{
							"PageNumber",
							"SizeLeft",
							"SizeUsed",
							"NumberOfEntries",
							"Level",
						}));
						GetSpace(readTree.State.RootPageNumber, tx, 0, stats, writer);
						writer.Flush();
					}

					long free = 0, used = 0;
					for (int i = 0; i < 4; i++)
					{
						free += stats[i].Free;
						used += stats[i].Used;
					}
					long overall = free + used;
				}*/




				//using (var r = tss.CreateReader())
				//{
				//	foreach (var point in r.Query("1234", DateTime.Now.AddDays(-1), DateTime.Now.AddMinutes(5)))
				//	{
				//		Console.WriteLine(point.At + " " + point.Value);
				//	}
				//}
			}
		}

		public class Stats
		{
			public long Free;
			public long Used;
			public int NumberOfPages;
			public int Level;
			public long NumberOfEntries;
		}

		private static unsafe void GetSpace(long p, Transaction tx, int index, Stats[] data)
		{
			if (data[index] == null)
			{
				data[index] = new Stats
				{
					Level = index
				};
			}
			var current = data[index];
			var readOnlyPage = tx.GetReadOnlyPage(p);
			current.Free += readOnlyPage.SizeLeft;
			current.Used+= readOnlyPage.SizeUsed;
			current.NumberOfPages++;
			current.NumberOfEntries += readOnlyPage.NumberOfEntries;
			/*writer.WriteLine(string.Join(",", new object[]
			{
				readOnlyPage.PageNumber,
				readOnlyPage.CalcSizeLeft(),
				readOnlyPage.CalcSizeUsed(),
				readOnlyPage.NumberOfEntries,
				current.Level,
			}));*/
			if (readOnlyPage.IsBranch)
			{
				for (int i = 0; i < readOnlyPage.NumberOfEntries; i++)
				{
					var nodeHeader = readOnlyPage.GetNode(i);
					GetSpace(nodeHeader->PageNumber, tx, index + 1, data);
				}
			}
		}

		private static void ImportWikipedia(TimeSeriesStorage tss, List<string> addedData)
		{
			var dir = @"E:\TimeSeries\20150401\Compressed";
			var files = Directory.GetFiles(dir, "pagecounts-*.gz", SearchOption.TopDirectoryOnly);
			for (int fileIndex = 0; fileIndex < files.Length; fileIndex++)
			{
				if (fileIndex > 30)
				{
					break;
				}
				var fileName = files[fileIndex];
				var path = fileName;

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

								var items = line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);

								if (items.Length < 4)
									continue;

								var entryName = items[0] + "/" + WebUtility.UrlDecode(items[1]);
								if (entryName.Length > 512)
									continue;

								var time = DateTime.ParseExact(fileName.Replace(@"E:\TimeSeries\20150401\Compressed\pagecounts-", "").Replace(".gz", ""), "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
								writer.Append("views/" + entryName, time, int.Parse(items[2]));
								//writer.Append("sizes/" + entryName, time, double.Parse(items[3]));

								if (lines++%1000 == 0)
								{
									Console.WriteLine(lines);
									writer.Commit();
									writer.Dispose();
									writer = tss.CreateWriter();
								}
							}
						}
					}
					finally
					{
						writer.Commit();
						writer.Dispose();
					}

					// addedData.Add(uncompressed.Length.ToString());
				}
			}
		}
	}
}