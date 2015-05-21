using System;
using System.Linq;
using System.Xml.Linq;
using Voron;
using Xunit;

namespace TimeSeries.Tests
{
	public class TimeSeriesTests
	{
		[Fact]
		public void CanQueryData()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(1),
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue
						});

					Assert.Equal(2, result.Count());
					var time = result.First().ToArray();
					var money = result.Last().ToArray();

					Assert.Equal(3, time.Length);
					Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[0].At);
					Assert.Equal(new DateTime(2015, 4, 1, 1, 0, 0), time[1].At);
					Assert.Equal(new DateTime(2015, 4, 1, 2, 0, 0), time[2].At);
					Assert.Equal(10, time[0].Value);
					Assert.Equal(19, time[1].Value);
					Assert.Equal(50, time[2].Value);
					Assert.Equal("Time", time[0].DebugKey);
					Assert.Equal("Time", time[1].DebugKey);
					Assert.Equal("Time", time[2].DebugKey);
					
					Assert.Equal(3, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal("Money", money[2].DebugKey);
				}
			}
		}

		[Fact]
		public void CanQueryDataOnSeries()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue
						});

					var money = result.Single().ToArray();
					Assert.Equal(3, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal("Money", money[2].DebugKey);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationSum()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(1),
							PeriodDuration = TimeSpan.FromHours(6),
							PeriodCalcOperation = CalcOperation.Sum,
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue,
							PeriodDuration = TimeSpan.FromHours(2),
							PeriodCalcOperation = CalcOperation.Sum,
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(1, time.Length);
					Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[0].At);
					Assert.Equal(79, time[0].Value);
					Assert.Equal("Time", time[0].DebugKey);
					Assert.Equal(TimeSpan.FromHours(6), time[0].Duration);

					Assert.Equal(2, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal(600, money[0].Value);
					Assert.Equal(130, money[1].Value);
					Assert.Equal(TimeSpan.FromHours(2), money[0].Duration);
					Assert.Equal(TimeSpan.FromHours(2), money[1].Duration);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationAverage()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(1),
							PeriodDuration = TimeSpan.FromHours(3),
							PeriodCalcOperation = CalcOperation.Average,
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue,
							PeriodDuration = TimeSpan.FromHours(2),
							PeriodCalcOperation = CalcOperation.Average,
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(1, time.Length);
					Assert.Equal("26.3333333333333", time[0].Value.ToString());
					Assert.Equal("Time", time[0].DebugKey);
					Assert.Equal(TimeSpan.FromHours(3), time[0].Duration);
					Assert.Equal(3, time[0].Candle.Volume);
					Assert.Equal(10, time[0].Candle.Open);
					Assert.Equal(50, time[0].Candle.Close);
					Assert.Equal(50, time[0].Candle.High);
					Assert.Equal(10, time[0].Candle.Low);
					

					Assert.Equal(2, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal(300, money[0].Value);
					Assert.Equal(130, money[1].Value);
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

		[Fact]
		public void CanQueryDataInSpecificDuration_Lower()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(1),
							PeriodDuration = TimeSpan.FromSeconds(3),
							PeriodCalcOperation = CalcOperation.Average,
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue,
							PeriodDuration = TimeSpan.FromMinutes(3),
							PeriodCalcOperation = CalcOperation.Sum,
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(3, time.Length);
					Assert.Equal(10, time[0].Value);
					Assert.Equal(19, time[1].Value);
					Assert.Equal(50, time[2].Value);
					Assert.Equal("Time", time[0].DebugKey);
					Assert.Equal(TimeSpan.FromSeconds(3), time[0].Duration);
					Assert.Equal(TimeSpan.FromSeconds(3), time[1].Duration);
					Assert.Equal(TimeSpan.FromSeconds(3), time[2].Duration);
					Assert.Equal(1, time[0].Candle.Volume);
					Assert.Equal(1, time[1].Candle.Volume);
					Assert.Equal(1, time[2].Candle.Volume);
					Assert.Equal(10, time[0].Candle.Open);
					Assert.Equal(19, time[1].Candle.Open);
					Assert.Equal(50, time[2].Candle.Open);
					Assert.Equal(10, time[0].Candle.Close);
					Assert.Equal(19, time[1].Candle.Close);
					Assert.Equal(50, time[2].Candle.Close);
					Assert.Equal(10, time[0].Candle.Low);
					Assert.Equal(19, time[1].Candle.Low);
					Assert.Equal(50, time[2].Candle.Low);
					Assert.Equal(10, time[0].Candle.High);
					Assert.Equal(19, time[1].Candle.High);
					Assert.Equal(50, time[2].Candle.High);


					Assert.Equal(3, money.Length);
					Assert.Equal(54, money[0].Value);
					Assert.Equal(546, money[1].Value);
					Assert.Equal(130, money[2].Value);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal(TimeSpan.FromMinutes(3), money[0].Duration);
					Assert.Equal(TimeSpan.FromMinutes(3), money[1].Duration);
					Assert.Equal(TimeSpan.FromMinutes(3), money[2].Duration);
					Assert.Equal(1, money[0].Candle.Volume);
					Assert.Equal(1, money[1].Candle.Volume);
					Assert.Equal(1, money[2].Candle.Volume);
					Assert.Equal(54, money[0].Candle.Open);
					Assert.Equal(546, money[1].Candle.Open);
					Assert.Equal(130, money[2].Candle.Open);
					Assert.Equal(54, money[0].Candle.Close);
					Assert.Equal(546, money[1].Candle.Close);
					Assert.Equal(130, money[2].Candle.Close);
					Assert.Equal(54, money[0].Candle.Low);
					Assert.Equal(546, money[1].Candle.Low);
					Assert.Equal(130, money[2].Candle.Low);
					Assert.Equal(54, money[0].Candle.High);
					Assert.Equal(546, money[1].Candle.High);
					Assert.Equal(130, money[2].Candle.High);
				}
			}
		}

		[Fact]
		public void MissingDataInSeries()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddSeconds(1),
							End = start.AddMinutes(30),
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = start.AddSeconds(1),
							End = start.AddMinutes(30),
						},
						new TimeSeriesQuery
						{
							Key = "Is",
							Start = start.AddSeconds(1),
							End = start.AddMinutes(30),
						}).ToArray();

					Assert.Equal(3, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();
					var Is = result[2].ToArray();

					Assert.Equal(0, time.Length);
					Assert.Equal(0, money.Length);
					Assert.Equal(0, Is.Length);
				}
			}
		}

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

			var writer = tss.CreateWriter();
			foreach (var item in data)
			{

				writer.Append(item.Key, item.At, item.Value);
			}

			writer.Commit();
			writer.Dispose();
		}
	}
}