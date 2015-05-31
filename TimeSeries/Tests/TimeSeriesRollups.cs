using System;
using System.Linq;
using Voron;
using Xunit;

namespace TimeSeries.Tests
{
	public class TimeSeriesRollups
	{
		private static void WriteTestData(TimeSeriesStorage tss)
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);
			var data = new[]
			{
				new {Key = "Time", At = start, Value = 10},
				new {Key = "Money", At = start, Value = 54},
				new {Key = "Is", At = start, Value = 1029},

				new {Key = "Money", At = start.AddHours(1), Value = 546},
				new {Key = "Is", At = start.AddHours(1), Value = 70},
				new {Key = "Time", At = start.AddHours(1), Value = 19},

				new {Key = "Is", At = start.AddHours(2), Value = 64},
				new {Key = "Money", At = start.AddHours(2), Value = 130},
				new {Key = "Time", At = start.AddHours(2), Value = 50},
			};

			using (var writer = tss.CreateWriter())
			{
				foreach (var item in data)
				{
					writer.Append(item.Key, item.At, item.Value);
				}
				writer.Commit();
			}
		}

		// TODO: When the source data is hourly, forbid querying on 1.5 hours period

		[Fact]
		public void CanQueryDataOnRollup()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				var r = tss.CreateReader();
				var result = r.Query(
					new TimeSeriesQuery
					{
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(1),
						PeriodDuration = TimeSpan.FromHours(1.5),
					},
					new TimeSeriesQuery
					{
						Key = "Money",
						Start = DateTime.MinValue,
						End = DateTime.MaxValue,
						PeriodDuration = TimeSpan.FromHours(2),
					}).ToArray();

				Assert.Equal(2, result.Length);
				var time = result[0].ToArray();
				var money = result[1].ToArray();

				Assert.Equal(2, time.Length);
				Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[0].At);
				Assert.Equal(new DateTime(2015, 4, 1, 2, 0, 0), time[1].At);
				Assert.Equal(29, time[0].Candle.Sum);
				Assert.Equal(50, time[1].Candle.Sum);
				Assert.Equal(double.NaN, time[0].Value);
				Assert.Equal(double.NaN, time[1].Value);
				Assert.Equal("Time", time[0].DebugKey);
				Assert.Equal("Time", time[1].DebugKey);

				Assert.Equal(2, money.Length);
				Assert.Equal("Money", money[0].DebugKey);
				Assert.Equal("Money", money[1].DebugKey);
				Assert.Equal(300, money[0].Candle.Sum / money[0].Candle.Volume);
				Assert.Equal(130, money[1].Candle.Sum / money[1].Candle.Volume);
				Assert.Equal(TimeSpan.FromHours(2), money[0].Duration);
				Assert.Equal(TimeSpan.FromHours(2), money[1].Duration);
				Assert.Equal(2, money[0].Candle.Volume);
				Assert.Equal(1, money[1].Candle.Volume);
				Assert.Equal(54, money[0].Candle.Open);
				Assert.Equal(130, money[1].Candle.Open);
				Assert.Equal(546, money[0].Candle.Close);
				Assert.Equal(130, money[1].Candle.Close);
				Assert.Equal(54, money[0].Candle.Low);
				Assert.Equal(130, money[1].Candle.Low);
				Assert.Equal(546, money[0].Candle.High);
				Assert.Equal(130, money[1].Candle.High);

				result = r.Query(
					new TimeSeriesQuery
					{
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(1),
						PeriodDuration = TimeSpan.FromHours(1.5),
					},
					new TimeSeriesQuery
					{
						Key = "Money",
						Start = DateTime.MinValue,
						End = DateTime.MaxValue,
						PeriodDuration = TimeSpan.FromHours(2),
					}).ToArray();

				Assert.Equal(2, result.Length);
				time = result[0].ToArray();
				money = result[1].ToArray();

				Assert.Equal(2, time.Length);
				Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[0].At);
				Assert.Equal(new DateTime(2015, 4, 1, 2, 0, 0), time[1].At);
				Assert.Equal(29, time[0].Candle.Sum);
				Assert.Equal(50, time[1].Candle.Sum);
				Assert.Equal(TimeSpan.FromHours(1.5), time[0].Duration);
				Assert.Equal(TimeSpan.FromHours(1.5), time[1].Duration);
				Assert.Equal(double.NaN, time[0].Value);
				Assert.Equal(double.NaN, time[1].Value);
				Assert.Equal("Time", time[0].DebugKey);
				Assert.Equal("Time", time[1].DebugKey);

				Assert.Equal(2, money.Length);
				Assert.Equal("Money", money[0].DebugKey);
				Assert.Equal("Money", money[1].DebugKey);
				Assert.Equal(300, money[0].Candle.Sum / money[0].Candle.Volume);
				Assert.Equal(130, money[1].Candle.Sum / money[1].Candle.Volume);
				Assert.Equal(TimeSpan.FromHours(2), money[0].Duration);
				Assert.Equal(TimeSpan.FromHours(2), money[1].Duration);
				Assert.Equal(2, money[0].Candle.Volume);
				Assert.Equal(1, money[1].Candle.Volume);
				Assert.Equal(54, money[0].Candle.Open);
				Assert.Equal(130, money[1].Candle.Open);
				Assert.Equal(546, money[0].Candle.Close);
				Assert.Equal(130, money[1].Candle.Close);
				Assert.Equal(54, money[0].Candle.Low);
				Assert.Equal(130, money[1].Candle.Low);
				Assert.Equal(546, money[0].Candle.High);
				Assert.Equal(130, money[1].Candle.High);
			}
		}
	}
}