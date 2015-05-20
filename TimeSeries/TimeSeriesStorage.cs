using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Voron;
using Voron.Debugging;
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
		}

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly Transaction _tx;
			private Tree _tree;

			public Reader(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.Read);
				_tree = _tx.State.GetTree(_tx, "data");
			}

			public IEnumerable<Point> Query(string key, DateTime start, DateTime end)
			{
				var sliceWriter = new SliceWriter(1024);
				sliceWriter.WriteString(key);
				sliceWriter.WriteBigEndian(start.Ticks);
				var startSlice = sliceWriter.CreateSlice();

				sliceWriter = new SliceWriter(1024);
				sliceWriter.WriteString(key);
				sliceWriter.WriteBigEndian(end.Ticks);
				var endSlice = sliceWriter.CreateSlice();

				var buffer = new byte[8];

				using (var it = _tree.Iterate())
				{
					it.MaxKey = endSlice;
				//	it.RequiredPrefix = key;
					if (it.Seek(startSlice) == false)
						yield break;

					do
					{
						var keyReader = it.CurrentKey.CreateReader();
						int used;
						var keyBytes = keyReader.ReadBytes(1024, out used);
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