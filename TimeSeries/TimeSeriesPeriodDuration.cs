using System;

namespace TimeSeries
{
	public class TimeSeriesPeriodDuration
	{
		public TimeSeriesPeriodDuration(PeriodType type, int duration)
		{
			Type = type;
			Duration = duration;
		}

		public PeriodType Type { get; private set; }
		
		public int Duration { get; private set; }

		public static TimeSeriesPeriodDuration Milliseconds(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Milliseconds, duration);
		}

		public static TimeSeriesPeriodDuration Seconds(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Seconds, duration);
		}

		public static TimeSeriesPeriodDuration Minutes(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Minutes, duration);
		}

		public static TimeSeriesPeriodDuration Hours(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Hours, duration);
		}

		public static TimeSeriesPeriodDuration Days(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Days, duration);
		}

		public static TimeSeriesPeriodDuration Weeks(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Weeks, duration);
		}

		public static TimeSeriesPeriodDuration Months(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Months, duration);
		}

		public static TimeSeriesPeriodDuration Years(int duration)
		{
			return new TimeSeriesPeriodDuration(PeriodType.Years, duration);
		}

		public override bool Equals(object obj)
		{
			var other = obj as TimeSeriesPeriodDuration;
			if (other == null)
				return false;

			return Type == other.Type &&
			       Duration == other.Duration;
		}

		public override int GetHashCode()
		{
			int hashCode = Type.GetHashCode();
			hashCode = (hashCode * 397) ^ Duration.GetHashCode();
			return hashCode;
		}

		public DateTime InRange(TimeSeriesQuery query)
		{
			throw new NotImplementedException();
		}

		public DateTime AddToDateTime(DateTime start)
		{
			switch (Type)
			{
				case PeriodType.Milliseconds:
					return start.AddMilliseconds(Duration);
				case PeriodType.Seconds:
					return start.AddSeconds(Duration);
				case PeriodType.Minutes:
					return start.AddMinutes(Duration);
				case PeriodType.Hours:
					return start.AddHours(Duration);
				case PeriodType.Days:
				case PeriodType.Weeks:
				case PeriodType.Months:
				case PeriodType.Years:
				default:
					throw new ArgumentOutOfRangeException();
			}
		}
	}
}