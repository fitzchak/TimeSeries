using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
#if DEBUG
			public string DebugKey { get; set; }
#endif
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
			private readonly Transaction _tx;
			private readonly Tree _tree;

			public Reader(TimeSeriesStorage storage)
			{
				_tx = storage._storageEnvironment.NewTransaction(TransactionFlags.Read);
				_tree = _tx.State.GetTree(_tx, "data");
			}

			public List<Point>[] Query(params TimeSeriesQuery[] queries)
			{
				var result = new List<Point>[queries.Length];
				Parallel.For(0, queries.Length, i =>
				{
					result[i] = GetQueryResult(queries[i]).ToList();
				});
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
						count = 1;
						SetupPointResult(durationStartPoint, duration, 1);
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
							case CalcOperation.Average:
								durationStartPoint.Value += point.Value;
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
						if (operation == CalcOperation.Average)
							durationStartPoint.Value = durationStartPoint.Value/count;

						yield return durationStartPoint;
						durationStartPoint = point;
						count = 1;
						SetupPointResult(durationStartPoint, duration, 1);
					}
				}

				if (durationStartPoint != null)
				{
					if (operation == CalcOperation.Average)
						durationStartPoint.Value = durationStartPoint.Value / count;

					yield return durationStartPoint;
				}
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
				var keyBytesLen = Encoding.UTF8.GetByteCount(query.Key) + sizeof(long);
				var startKeyWriter = new SliceWriter(keyBytesLen);
				startKeyWriter.WriteString(query.Key);
				var prefixKey = startKeyWriter.CreateSlice();
				startKeyWriter.WriteBigEndian(query.Start.Ticks);
				var startSlice = startKeyWriter.CreateSlice();

				var buffer = new byte[sizeof (double)];

				var endTicks = query.End.Ticks;

				using (var it = _tree.Iterate())
				{
					it.RequiredPrefix = prefixKey;
					
					if (it.Seek(startSlice) == false)
						yield break;

					do
					{
						if (it.CurrentKey.KeyLength != keyBytesLen) // avoid getting another key (A1, A10, etc)
							yield break;

						var keyReader = it.CurrentKey.CreateReader();
						var reader = it.CreateReaderForCurrent();

						reader.Read(buffer, 0, sizeof(double));

						keyReader.Skip(keyBytesLen - sizeof (long));
						var ticks = keyReader.ReadBigEndianInt64();
						if(ticks > endTicks)
							yield break;

						yield return new Point
						{
#if DEBUG
							DebugKey = keyReader.AsPartialSlice(sizeof (long)).ToString(),
#endif
							At = new DateTime(ticks),
							Value = EndianBitConverter.Big.ToDouble(buffer,0)
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
			private readonly Transaction _tx;

			private readonly byte[] keyBuffer = new byte[1024];
			private readonly byte[] valBuffer = new byte[8];
			private readonly Tree _tree;

			public Writer(TimeSeriesStorage storage)
			{
				_tx = storage._storageEnvironment.NewTransaction(TransactionFlags.ReadWrite); 
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