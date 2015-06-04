using System;

namespace TimeSeries
{
	public class RollupRange
	{
		public RollupRange(DateTime time)
		{
			Start = End = time;
		}

		public DateTime Start { get; set; }

		public DateTime End { get; set; }
	}
}