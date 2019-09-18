// Copyright © 2013-2015 Sergey Odinokov, Marco Casamento
// This software is based on https://github.com/HangfireIO/Hangfire.Redis

// Hangfire.Redis.StackExchange is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, either version 3
// of the License, or any later version.
//
// Hangfire.Redis.StackExchange is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with Hangfire.Redis.StackExchange. If not, see <http://www.gnu.org/licenses/>.

using System;
using Hangfire.Storage;
using StackExchange.Redis;
using Hangfire.Annotations;
using System.Threading;
using Hangfire.Common;

namespace Hangfire.Redis
{
    internal class RedisFetchedJob : IFetchedJob
    {
        private readonly RedisStorage _storage;
		private readonly IDatabase _redis;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private Timer _timer;

        public RedisFetchedJob(
            [NotNull] RedisStorage storage, 
            [NotNull] IDatabase redis,
            [NotNull] string jobId, 
            [NotNull] string queue)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redis = redis ?? throw new ArgumentNullException(nameof(redis));

            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));

            var halfLife = (int)(storage.InvisibilityTimeout.TotalMilliseconds / 2);
            _timer = new Timer(JobPing, null, halfLife, halfLife);
        }



        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            if (_storage.UseTransactions)
            {
                var transaction = _redis.CreateTransaction();
                RemoveFromFetchedList(transaction);
                transaction.Execute();                
            } else
            {
                RemoveFromFetchedList(_redis);
            }
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            if (_storage.UseTransactions)
            {
                var transaction = _redis.CreateTransaction();
                // prevent double entry for requeued job
                transaction.ListRemoveAsync(_storage.GetRedisKey($"queue:{Queue}"), JobId, -1);
                transaction.ListRightPushAsync(_storage.GetRedisKey($"queue:{Queue}"), JobId);
                RemoveFromFetchedList(transaction);
                transaction.Execute();
            } else
            {
                _redis.ListRemoveAsync(_storage.GetRedisKey($"queue:{Queue}"), JobId, -1);
                _redis.ListRightPushAsync(_storage.GetRedisKey($"queue:{Queue}"), JobId);
                RemoveFromFetchedList(_redis);
            }
            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            _timer.Dispose();

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }

        private void RemoveFromFetchedList(IDatabaseAsync databaseAsync)
        {
            databaseAsync.ListRemoveAsync(_storage.GetRedisKey($"queue:{Queue}:dequeued"), JobId, -1);
            databaseAsync.HashDeleteAsync(_storage.GetRedisKey($"job:{JobId}"), new RedisValue[] { "Fetched", "Checked", "Ping" });
        }

        private void JobPing(object state)
        {
            _redis.HashSet(
                    _storage.GetRedisKey($"job:{JobId}"),
                    "Ping",
                    JobHelper.SerializeDateTime(DateTime.UtcNow));
        }
    }
}
