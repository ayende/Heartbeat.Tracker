using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;

namespace Heartbeat.Tracker
{
	public class Program
	{
		static void Main()
		{
			var options = StorageEnvironmentOptions.CreateMemoryOnly();
			using (var strg = new HeartbeatStorage(options))
			{
				var sp = Stopwatch.StartNew();
				Parallel.For(0, 500 * 1000,i =>
				{
					var dateTime = DateTime.Now;
					var writer = strg.CreateWriter();
					for (int j = 0; j < 1; j++)
					{
						dateTime = dateTime.AddMinutes(1);
						writer.Track("users/" + j, dateTime, 5);
					}
					writer.Flush();
				});
				Console.WriteLine(sp.Elapsed);
				//sp.Restart();
				//Parallel.For(0, 1000, i =>
				//{
				//	using (var reader = strg.CreateReader())
				//	{
				//		{
				//			var count = reader
				//				.Read("users/" + i, DateTime.MinValue, DateTime.MaxValue)
				//				.Count();
				//		}
				//	}
				//});
				//Console.WriteLine(sp.Elapsed);
			}
		}
	}

	public class Reader : IDisposable
	{
		private readonly StorageEnvironment _storageEnvironment;
		private readonly Transaction _tx;

		public Reader(StorageEnvironment storageEnvironment, Transaction tx)
		{
			_storageEnvironment = storageEnvironment;
			_tx = tx;
		}

		public IEnumerable<KeyValuePair<DateTime, double>>
			Read(string watchId, DateTime from, DateTime to)
		{
			var readTree = _tx.ReadTree(watchId);
			using (var it = readTree.Iterate())
			{
				var end = new Slice(EndianBitConverter.Big.GetBytes(to.Ticks));
				it.MaxKey = end;
				var start = new Slice(EndianBitConverter.Big.GetBytes(@from.Ticks));
				if (it.Seek(start) == false)
					yield break;
				do
				{
					var time = new DateTime(it.CurrentKey.CreateReader().ReadBigEndianInt64());
					int _;
					var bytes = it.CreateReaderForCurrent().ReadBytes(8, out _);
					var d = BitConverter.ToDouble(bytes, 0);

					yield return new KeyValuePair<DateTime, double>(time, d);
				} while (it.MoveNext());
			}
		}

		public void Dispose()
		{
			_tx.Dispose();
		}
	}

	public class Writer
	{
		private readonly StorageEnvironment _env;
		private WriteBatch _writeBatch;

		public Writer(StorageEnvironment env)
		{
			_env = env;
			_writeBatch = new WriteBatch();
		}

		public void Track(string watchId, DateTime time, double val)
		{
			var sliceWriter = new SliceWriter(8);
			sliceWriter.WriteBigEndian(time.Ticks);
			var key = sliceWriter.CreateSlice();
			var valAsBytes = BitConverter.GetBytes(val);
			_writeBatch.Add(key, new MemoryStream(valAsBytes), watchId);
		}

		public void Flush()
		{
			_env.Writer.Write(_writeBatch);
			_writeBatch = new WriteBatch();
		}
	}
}
