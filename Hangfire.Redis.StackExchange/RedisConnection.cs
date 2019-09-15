// Copyright � 2013-2015 Sergey Odinokov, Marco Casamento
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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using StackExchange.Redis;
using Hangfire.Annotations;

namespace Hangfire.Redis
{
    internal class RedisConnection : JobStorageConnection
    {
        private readonly RedisStorage _storage;
        private readonly RedisSubscription _subscription;
        private readonly TimeSpan _fetchTimeout = TimeSpan.FromMinutes(3);

        public RedisConnection(
            [NotNull] RedisStorage storage,
            [NotNull] IDatabase redis,
            [NotNull] RedisSubscription subscription,
            TimeSpan fetchTimeout)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
            _fetchTimeout = fetchTimeout;

            Redis = redis ?? throw new ArgumentNullException(nameof(redis));
        }

        public IDatabase Redis { get; }
        
        public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            return RedisLock.Acquire(Redis, _storage.GetRedisKey(resource), timeout);
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            if (_storage.UseTransactions)
            {
                var transaction = Redis.CreateTransaction();

                transaction.SetAddAsync(_storage.GetRedisKey("servers"), serverId);

                transaction.HashSetAsync(
                    _storage.GetRedisKey($"server:{serverId}"),
                    new Dictionary<string, string>
                    {
                        { "WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture) },
                        { "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) },
                    }.ToHashEntries());

                if (context.Queues.Length > 0)
                {
                    transaction.ListRightPushAsync(
                        _storage.GetRedisKey($"server:{serverId}:queues"),
                        context.Queues.ToRedisValues());
                }

                transaction.Execute();
            } else
            {
                Redis.SetAddAsync(_storage.GetRedisKey("servers"), serverId);

                Redis.HashSetAsync(
                    _storage.GetRedisKey($"server:{serverId}"),
                    new Dictionary<string, string>
                    {
                        {"WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture)},
                        {"StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow)},
                    }.ToHashEntries());

                if (context.Queues.Length > 0)
                {
                    Redis.ListRightPushAsync(
                        _storage.GetRedisKey($"server:{serverId}:queues"),
                        context.Queues.ToRedisValues());
                }
            }
        }

        public override string CreateExpiredJob(
            [NotNull] Job job,
            [NotNull] IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var jobId = Guid.NewGuid().ToString();

            var invocationData = InvocationData.Serialize(job);

            // Do not modify the original parameters.
            var storedParameters = new Dictionary<string, string>(parameters)
            {
                { nameof(invocationData.Type), invocationData.Type },
                { nameof(invocationData.Method), invocationData.Method },
                { nameof(invocationData.ParameterTypes), invocationData.ParameterTypes },
                { nameof(invocationData.Arguments), invocationData.Arguments },
                { nameof(JobData.CreatedAt), JobHelper.SerializeDateTime(createdAt) }
            };

            if (_storage.UseTransactions)
            {
                var transaction = Redis.CreateTransaction();

                transaction.HashSetAsync(_storage.GetRedisKey($"job:{jobId}"), storedParameters.ToHashEntries());
                transaction.KeyExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);

                // TODO: check return value
                transaction.Execute();
            } else
            {
                Redis.HashSetAsync(_storage.GetRedisKey($"job:{jobId}"), storedParameters.ToHashEntries());
                Redis.KeyExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);
            }

            return jobId;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            if (_storage.UseTransactions)
            {
                return new RedisWriteOnlyTransaction(_storage, Redis.CreateTransaction());
            }
            return new RedisWriteDirectlyToDatabase(_storage, Redis);
        }

        public override void Dispose()
        {
            // nothing to dispose
        }

        public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            string jobId = null;
            string queueName = null;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i < queues.Length; i++)
                {
                    queueName = queues[i];
                    var queueKey = _storage.GetRedisKey($"queue:{queueName}");
                    var fetchedKey = _storage.GetRedisKey($"queue:{queueName}:dequeued");
                    jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);
                    if (jobId != null) break;
                }

                if (jobId == null)
                {
                    _subscription.WaitForJob(_fetchTimeout, cancellationToken);
                }
            }
            while (jobId == null);

            // The job was fetched by the server. To provide reliability,
            // we should ensure, that the job will be performed and acquired
            // resources will be disposed even if the server will crash
            // while executing one of the subsequent lines of code.

            // The job's processing is splitted into a couple of checkpoints.
            // Each checkpoint occurs after successful update of the
            // job information in the storage. And each checkpoint describes
            // the way to perform the job when the server was crashed after
            // reaching it.

            // Checkpoint #1-1. The job was fetched into the fetched list,
            // that is being inspected by the FetchedJobsWatcher instance.
            // Job's has the implicit 'Fetched' state.

            Redis.HashSet(
                _storage.GetRedisKey($"job:{jobId}"),
                "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow));

            // Checkpoint #2. The job is in the implicit 'Fetched' state now.
            // This state stores information about fetched time. The job will
            // be re-queued when the JobTimeout will be expired.

            return new RedisFetchedJob(_storage, Redis, jobId, queueName);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash([NotNull] string key)
        {
            var entries = Redis.HashGetAll(_storage.GetRedisKey(key));
            if (entries.Length == 0) return null;

            return entries.ToStringDictionary();
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            return Redis.ListRange(_storage.GetRedisKey(key)).ToStringList();
        }

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            HashSet<string> result = new HashSet<string>();
            foreach (var item in Redis.SortedSetScan(_storage.GetRedisKey(key)))
            {
                result.Add(item.Element);
            }
            
            return result;
        }

        public override long GetCounter([NotNull] string key)
        {
            return Convert.ToInt64(Redis.StringGet(_storage.GetRedisKey(key)));
        }

        public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            return Redis.SortedSetRangeByScore(_storage.GetRedisKey(key), fromScore, toScore, skip: 0, take: 1)
                .FirstOrDefault();
        }

        public override long GetHashCount([NotNull] string key)
        {
            return Redis.HashLength(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            return Redis.KeyTimeToLive(_storage.GetRedisKey(key)) ?? TimeSpan.Zero;
        }

        public override JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            
            var jobData = GetAllEntriesFromHash($"job:{jobId}");
            if (jobData == null) return null;
            
            var invocationData = new InvocationData(
                jobData.Get(nameof(InvocationData.Type)),
                jobData.Get(nameof(InvocationData.Method)), 
                jobData.Get(nameof(InvocationData.ParameterTypes)),
                jobData.Get(nameof(InvocationData.Arguments)));
                        
            var job = new JobData
            {
                State = jobData.Get("State"),
                CreatedAt = JobHelper.DeserializeNullableDateTime(jobData.Get(nameof(JobData.CreatedAt))) ?? DateTime.MinValue
            };
            
            try
            {
                job.Job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                job.LoadException = ex;
            }
            
            return job;
        }

        public override string GetJobParameter([NotNull] string jobId, [NotNull] string name)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Redis.HashGet(_storage.GetRedisKey($"job:{jobId}"), name);
        }

        public override long GetListCount([NotNull] string key)
        {
            return Redis.ListLength(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {
            return Redis.KeyTimeToLive(_storage.GetRedisKey(key)) ?? TimeSpan.Zero;
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            return Redis.ListRange(_storage.GetRedisKey(key), startingFrom, endingAt).ToStringList();
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            return Redis.SortedSetRangeByRank(_storage.GetRedisKey(key), startingFrom, endingAt).ToStringList();
        }

        public override long GetSetCount([NotNull] string key)
        {
            return Redis.SortedSetLength(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            return Redis.KeyTimeToLive(_storage.GetRedisKey(key)) ?? TimeSpan.Zero;
        }

        public override StateData GetStateData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            
            var stateData = GetAllEntriesFromHash($"job:{jobId}:state");
            if (stateData == null) return null;

            return new StateData
            {
                Name = stateData.Pull("State", true),
                Reason = stateData.Pull("Reason"),
                Data = stateData
            };
        }

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Redis.HashGet(_storage.GetRedisKey(key), name);
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Redis.HashSet(
                _storage.GetRedisKey($"server:{serverId}"),
                "Heartbeat",
                JobHelper.SerializeDateTime(DateTime.UtcNow));
        }

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            if (_storage.UseTransactions)
            {
                var transaction = Redis.CreateTransaction();

                transaction.SetRemoveAsync(_storage.GetRedisKey("servers"), serverId);

                transaction.KeyDeleteAsync(
                    new RedisKey[]
                    {
                    _storage.GetRedisKey($"server:{serverId}"),
                    _storage.GetRedisKey($"server:{serverId}:queues")
                    });

                transaction.Execute();
            } else
            {
                Redis.SetRemoveAsync(_storage.GetRedisKey("servers"), serverId);
                Redis.KeyDeleteAsync(
                    new RedisKey[]
                    {
                        _storage.GetRedisKey($"server:{serverId}"),
                        _storage.GetRedisKey($"server:{serverId}:queues")
                    });
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            var serverNames = Redis.SetMembers(_storage.GetRedisKey("servers"));
            var heartbeats = new Dictionary<string, Tuple<DateTime, DateTime?>>();

            var utcNow = DateTime.UtcNow;
            
            foreach (var serverName in serverNames)
            {
                var srv = Redis.HashGet(_storage.GetRedisKey($"server:{serverName}"), new RedisValue[] { "StartedAt", "Heartbeat" });
                heartbeats.Add(serverName,
                                new Tuple<DateTime, DateTime?>(
                                JobHelper.DeserializeDateTime(srv[0]),
                                JobHelper.DeserializeNullableDateTime(srv[1])));
            }

            var removedServerCount = 0;
            foreach (var heartbeat in heartbeats)
            {
                var maxTime = new DateTime(
                    Math.Max(heartbeat.Value.Item1.Ticks, (heartbeat.Value.Item2 ?? DateTime.MinValue).Ticks));

                if (utcNow > maxTime.Add(timeOut))
                {
                    RemoveServer(heartbeat.Key);
                    removedServerCount++;
                }
            }

            return removedServerCount;
        }

        public override void SetJobParameter([NotNull] string jobId, [NotNull] string name, string value)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (name == null) throw new ArgumentNullException(nameof(name));

            Redis.HashSet(_storage.GetRedisKey($"job:{jobId}"), name, value);
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            Redis.HashSet(_storage.GetRedisKey(key), keyValuePairs.ToHashEntries());
        }
    }
}
