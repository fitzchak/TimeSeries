using System;
using System.Diagnostics;
using Voron;

namespace TimeSeries
{
	public class Program
	{
		private static void Main(string[] args)
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath("B2");
			//storageEnvironmentOptions.ManualFlushing = true;
			using (var tss = new TimeSeriesStorage(storageEnvironmentOptions))
			{
				Console.WriteLine("running");
				var sp = Stopwatch.StartNew();

				int txCount = 0;
				var count = 5;
				var now = DateTime.Now;
				count = DoWrites(tss, now, count, ref txCount);

				sp.Stop();
				Console.WriteLine(sp.Elapsed);
				Console.WriteLine("Num: {0:#,#}, TxCount: {1:#,#}", count, txCount);
				Console.WriteLine(Math.Round((double)count/ sp.ElapsedMilliseconds, 4));

				/*using (var r = tss.CreateReader())
				{
					foreach (var point in r.Query("1234", DateTime.Now.AddDays(-1), DateTime.Now.AddMinutes(5)))
					{
						Console.WriteLine(point.At + " " + point.Value);
					}
				}*/
			}
		}

		private static int DoWrites(TimeSeriesStorage tss, DateTime now, int count, ref int txCount)
		{
			for (int j = 0; j < 20*1000; j++)
			{
				using (var w = tss.CreateWriter())
				{
					for (int i = 0; i < 100; i++)
					{
						now = now.AddMinutes(1);
						w.Append("one", now, i);
						w.Append("two", now, i);
						w.Append("three", now, i);
						w.Append("four", now, i);
						w.Append("five", now, i);
						count += 5;
					}
					txCount++;
					w.Commit();
				}
			}
			return count;
		}
	}
}