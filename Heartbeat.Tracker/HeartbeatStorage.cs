using System;
using Voron;

namespace Heartbeat.Tracker
{
	public class HeartbeatStorage : IDisposable
	{
		private StorageEnvironment _storageEnvironment;

		public HeartbeatStorage(StorageEnvironmentOptions options)
		{
			_storageEnvironment = new StorageEnvironment(options);
		}

		public Writer CreateWriter()
		{
			return new Writer(_storageEnvironment);
		}

		public Reader CreateReader()
		{
			var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read);
			return new Reader(_storageEnvironment, tx);
		}

		public void Dispose()
		{
			if (_storageEnvironment != null)
				_storageEnvironment.Dispose();
		}
	}
}