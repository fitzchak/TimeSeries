using System;
using System.Linq;
using Voron;
using Xunit;

namespace TimeSeries.Tests
{
	public class TimeSeriesRollupsOutOfRange
	{
		[Fact]
		public void HourlyData_QueryPer3Hours_StartedAt4()
		{
			var start = new DateTime(2015, 4, 1, 4, 0, 0);

			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				var r = tss.CreateReader();
				Assert.Throws<InvalidOperationException>(() =>
				{
					r.Query(new TimeSeriesQuery
					{
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(1),
						PeriodDuration = TimeSeriesPeriodDuration.Hours(3),
					}).ToArray();
				});
				
			}
		}

		[Fact]
		public void HourlyData_QueryPer2Hours_StartedAt9()
		{
			var start = new DateTime(2015, 4, 1, 9, 0, 0);

			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				var r = tss.CreateReader();
				Assert.Throws<InvalidOperationException>(() =>
				{
					r.Query(new TimeSeriesQuery
					{
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(1),
						PeriodDuration = TimeSeriesPeriodDuration.Hours(2),
					}).ToArray();
				});

			}
		}
	}
}