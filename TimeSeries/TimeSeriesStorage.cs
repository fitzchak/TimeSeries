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
			return new Reader(this, true);
		}

		public Writer CreateWriter()
		{
			return new Writer(this);
		}

		public class Point
		{
			private double _value;
#if DEBUG
			public string DebugKey { get; set; }
#endif
			public DateTime At { get; set; }

			public double Value
			{
				get { return _value; }
				set { _value = value; }
			}

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

			public double Sum { get; set; }
		}

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly bool _storeComputedPeriods;
			private readonly Transaction _tx;
			private readonly Tree _tree;

			public Reader(TimeSeriesStorage storage, bool storeComputedPeriods)
			{
				_storage = storage;
				_storeComputedPeriods = storeComputedPeriods;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.Read);
				_tree = _tx.State.GetTree(_tx, "data");
			}

			public IEnumerable<Point> Query(TimeSeriesQuery query)
			{
				return GetQueryResult(query);
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
				if (query.PeriodDuration.HasValue == false)
				{
					var result = GetRawQueryResult(query, _tree);
					return result;
				}
				
				using (var periodTx = _storage._storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
				{
					var periodTree = periodTx.State.GetTree(periodTx, "period_" + query.PeriodDuration.Value.Ticks);
					var result = GetRawQueryResult(query, periodTree).ToArray();
					if (result.Any())
					{
						return result;
					}

					var computedResult = GetRawQueryResult(query, _tree);
					var computedResultArray = AnalyzePeriodDuration(computedResult, query.PeriodDuration.Value).ToArray().ToArray();

					using (var writer = new RollupWriter(periodTree))
					{
						foreach (var point in computedResultArray)
						{
							writer.Append(query.Key, point.At, point.Candle);
						}
						periodTx.Commit();
					}

					return computedResultArray;
				}
			}

			private IEnumerable<Point> AnalyzePeriodDuration(IEnumerable<Point> result, TimeSpan duration)
			{
				Point durationStartPoint = null;

				int count = 0;
				foreach (var point in result)
				{
					if (durationStartPoint == null)
					{
						count = 1;
						durationStartPoint = SetupPointResult(point, duration, 1);
						continue;
					}

					if (point.At - durationStartPoint.At < duration)
					{
						durationStartPoint.Candle.High = Math.Max(durationStartPoint.Candle.High, point.Value);
						durationStartPoint.Candle.Low = Math.Min(durationStartPoint.Candle.Low, point.Value);
						durationStartPoint.Candle.Close = point.Value;
						durationStartPoint.Candle.Volume = ++count;
						durationStartPoint.Candle.Sum += point.Value;
						durationStartPoint.Value = double.NaN;
					}
					else
					{
						yield return durationStartPoint;
						count = 1;
						durationStartPoint = SetupPointResult(point, duration, 1); ;
					}
				}

				if (durationStartPoint != null)
				{
					yield return durationStartPoint;
				}
			}

			private Point SetupPointResult(Point point, TimeSpan duration, int count)
			{
				return new Point
				{
#if DEBUG
					DebugKey = point.DebugKey,
#endif
					At = point.At,
					Value = double.NaN,
					Duration = duration,
					Candle = new Candle
					{
						Open = point.Value,
						High = point.Value,
						Low = point.Value,
						Close = point.Value,
						Volume = count,
						Sum = point.Value,
					}
				};
			}

			private static IEnumerable<Point> GetRawQueryResult(TimeSeriesQuery query, Tree tree)
			{
				var isRawData = tree.Name == "data";

				var keyBytesLen = Encoding.UTF8.GetByteCount(query.Key) + sizeof(long);
				var startKeyWriter = new SliceWriter(keyBytesLen);
				startKeyWriter.WriteString(query.Key);
				var prefixKey = startKeyWriter.CreateSlice();
				startKeyWriter.WriteBigEndian(query.Start.Ticks);
				var startSlice = startKeyWriter.CreateSlice();

				var buffer = new byte[sizeof (double)];

				var endTicks = query.End.Ticks;

				using (var it = tree.Iterate())
				{
					it.RequiredPrefix = prefixKey;
					
					if (it.Seek(startSlice) == false)
						yield break;

					do
					{
						if (it.CurrentKey.KeyLength != keyBytesLen) // avoid getting another key (A1, A10, etc)
							yield break;

						var keyReader = it.CurrentKey.CreateReader();
						keyReader.Skip(keyBytesLen - sizeof (long));
						var ticks = keyReader.ReadBigEndianInt64();
						if(ticks > endTicks)
							yield break;

						var point = new Point
						{
#if DEBUG
							DebugKey = keyReader.AsPartialSlice(sizeof(long)).ToString(),
#endif
							At = new DateTime(ticks),
						};

						if (isRawData)
						{
							var reader = it.CreateReaderForCurrent();
							reader.Read(buffer, 0, sizeof (double));
							point.Value = EndianBitConverter.Big.ToDouble(buffer, 0);
						}
						else
						{
							var structureReader = it.ReadStructForCurrent(RollupWriter.CandleSchema);
							var candle = new Candle
							{
								High = structureReader.ReadDouble(PointCandleSchema.High),
								Low = structureReader.ReadDouble(PointCandleSchema.Low),
								Open = structureReader.ReadDouble(PointCandleSchema.Open),
								Close = structureReader.ReadDouble(PointCandleSchema.Close),
								Sum = structureReader.ReadDouble(PointCandleSchema.Sum),
								Volume = structureReader.ReadInt(PointCandleSchema.Volume),
							};
							point.Candle = candle;
							point.Duration = query.PeriodDuration;
							point.Value = double.NaN;
						}

						yield return point;
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

		public class RollupWriter : IDisposable
		{
			public static readonly StructureSchema<PointCandleSchema> CandleSchema;

			static RollupWriter()
			{
				CandleSchema = new StructureSchema<PointCandleSchema>()
					.Add<double>(PointCandleSchema.High)
					.Add<double>(PointCandleSchema.Low)
					.Add<double>(PointCandleSchema.Open)
					.Add<double>(PointCandleSchema.Close)
					.Add<double>(PointCandleSchema.Sum)
					.Add<int>(PointCandleSchema.Volume);
			}

			private readonly Tree _tree;

			private readonly byte[] _keyBuffer = new byte[1024];

			public RollupWriter(Tree tree)
			{
				_tree = tree;
			}

			public void Append(string key, DateTime time, Candle candle)
			{
				var sliceWriter = new SliceWriter(_keyBuffer);
				sliceWriter.WriteString(key);
				sliceWriter.WriteBigEndian(time.Ticks);
				var keySlice = sliceWriter.CreateSlice();

				var structure = new Structure<PointCandleSchema>(CandleSchema);

				structure.Set(PointCandleSchema.High, candle.High);
				structure.Set(PointCandleSchema.Low, candle.Low);
				structure.Set(PointCandleSchema.Open, candle.Open);
				structure.Set(PointCandleSchema.Close, candle.Close);
				structure.Set(PointCandleSchema.Sum, candle.Sum);
				structure.Set(PointCandleSchema.Volume, candle.Volume);

				_tree.WriteStruct(keySlice, structure);
			}

			public void Dispose()
			{
			}
		}

		public void Dispose()
		{
			if (_storageEnvironment != null)
				_storageEnvironment.Dispose();
		}
	}
}