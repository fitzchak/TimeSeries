using System;
using System.Collections.Generic;
using System.IO;
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
			public DateTime At { get; set; }

			public double Value { get; set; }
		}

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly Transaction _tx;

			public Reader(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.Read);
			}

			public IEnumerable<Point> Query(string treeName, DateTime start, DateTime end)
			{
				var startBuffer = EndianBitConverter.Big.GetBytes(start.Ticks);
				var endBuffer = EndianBitConverter.Big.GetBytes(end.Ticks);

				var buffer = new byte[8];

				var tree = _tx.State.GetTree(_tx, treeName);
				using (var it = tree.Iterate())
				{
					it.MaxKey = new Slice(endBuffer);
					if (it.Seek(new Slice(startBuffer)) == false)
						yield break;

					do
					{
						var reader = it.CreateReaderForCurrent();
						reader.Read(buffer, 0, 8);

						yield return new Point
						{
							At = new DateTime(it.CurrentKey.CreateReader().ReadBigEndianInt64()),
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

			private readonly byte[] keyBuffer = new byte[8];
			private readonly byte[] valBuffer = new byte[8];

			private readonly Dictionary<string,Tree> _trees =new Dictionary<string, Tree>();

			public Writer(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
			}

			public void Append(string treeName, DateTime time, double value)
			{
				Tree tree;
				if (_trees.TryGetValue(treeName, out tree) == false)
				{
					tree = _tx.State.GetTree(_tx, treeName);
					_trees[treeName] = tree;
				}
				EndianBitConverter.Big.CopyBytes(time.Ticks, keyBuffer, 0);
				EndianBitConverter.Big.CopyBytes(value, valBuffer, 0);

				tree.Add(new Slice(keyBuffer), valBuffer);
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