using System;

namespace TimeSeries
{
	public class PeriodDuration
	{
		public PeriodDuration(PeriodType type, int duration)
		{
			Type = type;
			Duration = duration;
		}

		public PeriodType Type { get; private set; }
		
		public int Duration { get; private set; }

		public static PeriodDuration Seconds(int duration)
		{
			return new PeriodDuration(PeriodType.Seconds, duration);
		}

		public static PeriodDuration Minutes(int duration)
		{
			return new PeriodDuration(PeriodType.Minutes, duration);
		}

		public static PeriodDuration Hours(int duration)
		{
			return new PeriodDuration(PeriodType.Hours, duration);
		}

		public static PeriodDuration Days(int duration)
		{
			return new PeriodDuration(PeriodType.Days, duration);
		}

		public static PeriodDuration Weeks(int duration)
		{
			return new PeriodDuration(PeriodType.Weeks, duration);
		}

		public static PeriodDuration Months(int duration)
		{
			return new PeriodDuration(PeriodType.Months, duration);
		}

		public static PeriodDuration Years(int duration)
		{
			return new PeriodDuration(PeriodType.Years, duration);
		}

		public override bool Equals(object obj)
		{
			var other = obj as PeriodDuration;
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
				case PeriodType.Seconds:
					return start.AddSeconds(Duration);
				case PeriodType.Minutes:
					return start.AddMinutes(Duration);
				case PeriodType.Hours:
					return start.AddHours(Duration);
				case PeriodType.Days:
					return start.AddDays(Duration);
				case PeriodType.Weeks:
					return start.AddDays(Duration * 7);
				case PeriodType.Months:
					return start.AddMonths(Duration);
				case PeriodType.Years:
					return start.AddYears(Duration);
				default:
					throw new ArgumentOutOfRangeException();
			}
		}
	}
}