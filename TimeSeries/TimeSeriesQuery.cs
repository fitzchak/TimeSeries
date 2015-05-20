using System;

namespace TimeSeries
{
	public class TimeSeriesQuery
	{
		public string Key { get; set; }

		public DateTime Start { get; set; }
		
		public DateTime End { get; set; }
	}
}