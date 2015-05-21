using System;
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
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("Test3");
			//storageEnvironmentOptions.ManualFlushing = true;
			using (var tss = new TimeSeriesStorage(storageEnvironmentOptions))
			{
				Console.WriteLine("running");

				// WriteTestData(tss);

				/*var sp = Stopwatch.StartNew();
				ImportWikipedia(tss);
				sp.Stop();
				Console.WriteLine(sp.Elapsed);*/

				/*using (var tx = tss.StorageEnvironment.NewTransaction(TransactionFlags.Read))
				{
					var readTree = tx.ReadTree("data");

					var stats = new Stats[10];
					using (var fileStream = File.Create("DataContent.csv"))
					using (var writer = new StreamWriter(fileStream, Encoding.UTF8))
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

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					foreach (var result in r.Query(
						new TimeSeriesQuery
						{
							Key = "views/en/Time", 
							Start = start.AddYears(-1), 
							End = start.AddYears(1).AddDays(1), 
							PeriodDuration = TimeSpan.FromHours(6),
							PeriodCalcOperation = CalcOperation.Sum,
						},
						new TimeSeriesQuery {Key = "sizes/en/Time", Start = DateTime.MinValue, End = DateTime.MaxValue})
						)
					{
						foreach (var point in result)
						{
							Console.WriteLine(point.DebugKey + ": " + point.At + " - " + point.Value);
						}
					}
				}
			}
		}

		private static void WriteTestData(TimeSeriesStorage tss)
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);
			var data = new[]
			{
				new {Key = "Time", At = start, Value = 10},
				new {Key = "Money", At = start, Value = 10},
				new {Key = "Is", At = start, Value = 10},

				new {Key = "Money", At = start.AddHours(1), Value = 20},
				new {Key = "Is", At = start.AddHours(1), Value = 20},
				new {Key = "Time", At = start.AddHours(1), Value = 20},

				new {Key = "Money", At = start.AddHours(2), Value = 30},
				new {Key = "Is", At = start.AddHours(2), Value = 30},
				new {Key = "Time", At = start.AddHours(2), Value = 30},
			};

			var writer = tss.CreateWriter();
			foreach (var item in data)
			{

				writer.Append("views/en/" + item.Key, item.At, item.Value);
			}

			writer.Commit();
			writer.Dispose();
		}

/*
		public class Stats
		{
			public long Free;
			public long Used;
			public int NumberOfPages;
			public int Level;
			public long NumberOfEntries;
		}

		private static unsafe void GetSpace(long p, Transaction tx, int index, Stats[] data, StreamWriter writer)
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
			current.Used += readOnlyPage.SizeUsed;
			current.NumberOfPages++;
			current.NumberOfEntries += readOnlyPage.NumberOfEntries;
			writer.WriteLine(string.Join(",", new object[]
			{
				readOnlyPage.PageNumber,
				readOnlyPage.CalcSizeLeft(),
				readOnlyPage.CalcSizeUsed(),
				readOnlyPage.NumberOfEntries,
				current.Level,
			}));
			if (readOnlyPage.IsBranch)
			{
				for (int i = 0; i < readOnlyPage.NumberOfEntries; i++)
				{
					var nodeHeader = readOnlyPage.GetNode(i);
					GetSpace(nodeHeader->PageNumber, tx, index + 1, data, writer);
				}
			}
		}*/

		private static void ImportWikipedia(TimeSeriesStorage tss)
		{
			var dir = @"E:\TimeSeries\20150401\Compressed";
			var files = Directory.GetFiles(dir, "pagecounts-*.gz", SearchOption.TopDirectoryOnly);
			for (int fileIndex = 0; fileIndex < files.Length; fileIndex++)
			{
				Console.WriteLine("Importing " + fileIndex);

				if (fileIndex > 2)
					break;

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
						writer.Commit();
						writer.Dispose();
					}
				}
			}
		}
	}
}