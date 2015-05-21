using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace TimeSeries
{
	public class TimeSeriesStorage : IDisposable
	{
		private readonly StorageEnvironment _storageEnvironment;

		public Guid Id { get; set; }

		public TimeSeriesStorage(StorageEnvironmentOptions options)
		{
			_storageEnvironment = new StorageEnvironment(options);
		//	_storageEnvironment.DebugJournal = new DebugJournal("debug_journal_test", _storageEnvironment, true);

			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = _storageEnvironment.CreateTree(tx, "$metadata");
				_storageEnvironment.CreateTree(tx, "data", keysPrefixing: false);
				var result = metadata.Read("id");
				if (result == null) // new db
				{
					Id = Guid.NewGuid();
					metadata.Add("id", new MemoryStream(Id.ToByteArray()));
				}
				else
				{
					int used;
					Id = new Guid(result.Reader.ReadBytes(16, out used));
				}

				tx.Commit();
			}
		}

		public StorageEnvironment StorageEnvironment
		{
			get { return _storageEnvironment; }
		}

		public Reader CreateReader()
		{
			return new Reader(this);
		}

		public Writer CreateWriter()
		{
			return new Writer(this);
		}

		public class Point
		{
			public string Key { get; set; }

			public DateTime At { get; set; }

			public double Value { get; set; }

			public TimeSpan? Duration { get; set; }

			public Candle Candle { get; set; }
		}

		public class Candle
		{
			public double High { get; set; }
			
			public double Low { get; set; }
			
			public double Open { get; set; }
			
			public double Close { get; set; }

			public int Volume { get; set; }
		}

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly Transaction _tx;
			private readonly Tree _tree;

			public Reader(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.Read);
				_tree = _tx.State.GetTree(_tx, "data");
			}

			public IEnumerable<Point>[] Query(params TimeSeriesQuery[] queries)
			{
				var result = new IEnumerable<Point>[queries.Length];
				for (int i = 0; i < queries.Length; i++)
				{
					result[i] = GetQueryResult(queries[i]);
				}
				return result;
			}

			private IEnumerable<Point> GetQueryResult(TimeSeriesQuery query)
			{
				var result = GetRawQueryResult(query);

				if (query.PeriodDuration.HasValue)
					result = AnalyzePeriodDuration(result, query.PeriodDuration.Value, query.PeriodCalcOperation);

				return result;
			}

			private IEnumerable<Point> AnalyzePeriodDuration(IEnumerable<Point> result, TimeSpan duration, CalcOperation operation)
			{
				Point durationStartPoint = null;

				int count = 0;
				foreach (var point in result)
				{
					if (durationStartPoint == null)
					{
						durationStartPoint = point;
						SetupPointResult(durationStartPoint, duration, count = 1);
						continue;
					}

					if (point.At - durationStartPoint.At < duration)
					{
						durationStartPoint.Candle.High = Math.Max(durationStartPoint.Candle.High, point.Value);
						durationStartPoint.Candle.Low = Math.Min(durationStartPoint.Candle.Low, point.Value);
						durationStartPoint.Candle.Close = point.Value;
						durationStartPoint.Candle.Volume = ++count;

						switch (operation)
						{
							case CalcOperation.Sum:
								durationStartPoint.Value += point.Value;
								break;
							case CalcOperation.Average:
								var previousAvarage = durationStartPoint.Value;
								durationStartPoint.Value = previousAvarage*(count - 1)/(count) + point.Value/(count); 
								break;
							case CalcOperation.Min:
								durationStartPoint.Value += Math.Min(durationStartPoint.Value, point.Value);
								break;
							case CalcOperation.Max:
								durationStartPoint.Value += Math.Max(durationStartPoint.Value, point.Value);
								break;
							default:
								throw new ArgumentOutOfRangeException();
						}
					}
					else
					{
						yield return durationStartPoint;
						durationStartPoint = point;
						SetupPointResult(durationStartPoint, duration, count = 1);
					}
				}

				if (durationStartPoint != null)
					yield return durationStartPoint;
			}

			private void SetupPointResult(Point point, TimeSpan duration, int count)
			{
				point.Duration = duration;
				point.Candle = new Candle
				{
					Open = point.Value,
					High = point.Value,
					Low = point.Value,
					Close = point.Value,
					Volume = count
				};
			}

			private IEnumerable<Point> GetRawQueryResult(TimeSeriesQuery query)
			{
				var sliceWriter = new SliceWriter(1024);
				sliceWriter.WriteString(query.Key);
				sliceWriter.WriteBigEndian(query.Start.Ticks);
				var startSlice = sliceWriter.CreateSlice();

				var buffer = new byte[8];

				using (var it = _tree.Iterate())
				{
					it.RequiredPrefix = query.Key;
					if (it.Seek(startSlice) == false)
						yield break;

					var keyLength = Encoding.UTF8.GetByteCount(query.Key);
					do
					{
						var keyReader = it.CurrentKey.CreateReader();
						int used;
						var keyBytes = keyReader.ReadBytes(1024, out used);
						if (used > keyLength + 8)
							yield break;
						var timeTicks = EndianBitConverter.Big.ToInt64(keyBytes, used - 8);

						var reader = it.CreateReaderForCurrent();
						reader.Read(buffer, 0, 8);

						yield return new Point
						{
							Key = Encoding.UTF8.GetString(keyBytes, 0, used - 8),
							At = new DateTime(timeTicks),
							Value = EndianBitConverter.Big.ToDouble(buffer, 0)
						};
					} while (it.MoveNext());
				}
			}

			public void Dispose()
			{
				if (_tx != null)
					_tx.Dispose();
			}
		}

		public class Writer : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly Transaction _tx;

			private readonly byte[] keyBuffer = new byte[1024];
			private readonly byte[] valBuffer = new byte[8];
			private readonly Tree _tree;

			public Writer(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.ReadWrite); 
				_tree = _tx.State.GetTree(_tx, "data");
			}

			public void Append(string key, DateTime time, double value)
			{
				var sliceWriter = new SliceWriter(keyBuffer);
				sliceWriter.WriteString(key);
				sliceWriter.WriteBigEndian(time.Ticks);
				var keySlice = sliceWriter.CreateSlice();

				EndianBitConverter.Big.CopyBytes(value, valBuffer, 0);

				_tree.Add(keySlice, valBuffer);
			}

			public void Dispose()
			{
				if (_tx != null)
					_tx.Dispose();
			}

			public void Commit()
			{
				_tx.Commit();
			}
		}

		public void Dispose()
		{
			if (_storageEnvironment != null)
				_storageEnvironment.Dispose();
		}
	}
}