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
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Hangfire.Annotations;
using StackExchange.Redis;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;

namespace Hangfire.Redis
{
    public class RedisMonitoringApi : IMonitoringApi
    {
        private const TaskContinuationOptions ContinuationOptions = TaskContinuationOptions.OnlyOnRanToCompletion |
                                                                    TaskContinuationOptions.ExecuteSynchronously;
        
        private readonly RedisStorage _storage;
        private readonly IDatabase _database;

		public RedisMonitoringApi([NotNull] RedisStorage storage, [NotNull] IDatabase database)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_database = database ?? throw new ArgumentNullException(nameof(database));
        }

        public long ScheduledCount()
        {
            return UseConnection(redis =>
                redis.SortedSetLength(_storage.GetRedisKey("schedule")));
        }

        public long EnqueuedCount([NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis => redis.ListLength(_storage.GetRedisKey($"queue:{queue}")));
        }

        public long FetchedCount([NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis => redis.ListLength(_storage.GetRedisKey($"queue:{queue}:dequeued")));
        }

        public long ProcessingCount() => UseConnection(redis => redis.SortedSetLength(_storage.GetRedisKey("processing")));

        public long SucceededListCount() => UseConnection(redis => redis.ListLength(_storage.GetRedisKey("succeeded")));

        public long FailedCount() => UseConnection(redis => redis.SortedSetLength(_storage.GetRedisKey("failed")));

        public long DeletedListCount() => UseConnection(redis => redis.ListLength(_storage.GetRedisKey("deleted")));

        public IDatabase GetDataBase() => _database;

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count) => UseConnection(redis 
            =>
        {
            var jobIds = redis
                .SortedSetRangeByRank(_storage.GetRedisKey("processing"), from, from + count - 1)
                .ToStringArray();

            return new JobList<ProcessingJobDto>(GetJobsWithProperties(redis,
                jobIds,
                null,
                new[] { "StartedAt", "ServerName", "ServerId", "State" },
                (job, jobData, state) => new ProcessingJobDto
                {
                    ServerId = state[2] ?? state[1],
                    Job = job,
                    StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                    InProcessingState = ProcessingState.StateName.Equals(
                        state[3], StringComparison.OrdinalIgnoreCase),
                })
                .Where(x => x.Value?.ServerId != null)
                .OrderBy(x => x.Value.StartedAt));
        });

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count) => UseConnection(redis =>
            {
                var scheduledJobs = redis
                    .SortedSetRangeByRankWithScores(_storage.GetRedisKey("schedule"), from, from + count - 1);

                if (scheduledJobs.Length == 0)
                {
                    return new JobList<ScheduledJobDto>(Enumerable.Empty<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobProperties = new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" };
                var stateProperties = new RedisValue[] { "State", "ScheduledAt" };

                var jobs = new ConcurrentDictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
                var states = new ConcurrentDictionary<string, string[]>(StringComparer.OrdinalIgnoreCase); ;

                var pipeline = redis.CreateBatch();
                var tasks = new Task[scheduledJobs.Length * 2];

                var i = 0;
                foreach (var job in scheduledJobs)
                {
                    var jobId = (string)job.Element;

                    tasks[i++] = pipeline.HashGetAsync(_storage.GetRedisKey($"job:{jobId}"), jobProperties)
                        .ContinueWith(x => jobs.TryAdd(jobId, x.Result.ToStringArray()), ContinuationOptions);

                    tasks[i++] = pipeline.HashGetAsync(_storage.GetRedisKey($"job:{jobId}:state"), stateProperties)
                        .ContinueWith(x => states.TryAdd(jobId, x.Result.ToStringArray()), ContinuationOptions);
                }

                pipeline.Execute();
                pipeline.WaitAll(tasks);

                return new JobList<ScheduledJobDto>(scheduledJobs
                    .Select(job =>
                    {
                        var jobId = (string)job.Element;
                        var state = states[jobId];

                        return new KeyValuePair<string, ScheduledJobDto>(jobId, new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long)job.Score),
                            Job = TryToGetJob(jobs[jobId], 0),
                            ScheduledAt = JobHelper.DeserializeNullableDateTime(state[1]),
                            InScheduledState = ScheduledState.StateName.Equals(state[0], StringComparison.OrdinalIgnoreCase)
                        });
                    }));
            });

        public IDictionary<DateTime, long> SucceededByDatesCount() => UseConnection(redis => GetTimelineStats(redis, "succeeded"));

        public IDictionary<DateTime, long> FailedByDatesCount() => UseConnection(redis => GetTimelineStats(redis, "failed"));

        public IList<ServerDto> Servers()
        {
            return UseConnection(redis =>
            {
                var serverNames = redis
                    .SetMembers(_storage.GetRedisKey("servers"))
                    .ToStringArray();

                if (serverNames.Length == 0)
                {
                    return new List<ServerDto>();
                }

                var serverProperties = new RedisValue[] { "WorkerCount", "StartedAt", "Heartbeat" };

                var servers = new ConcurrentDictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
                var queues = new ConcurrentDictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

                var pipeline = redis.CreateBatch();
                var tasks = new Task[serverNames.Length * 2];

                var i = 0;
                foreach (var name in serverNames)
                {
                    var serverName = name;

                    tasks[i++] = pipeline.HashGetAsync(_storage.GetRedisKey($"server:{serverName}"), serverProperties)
                        .ContinueWith(x => servers.TryAdd(serverName, x.Result.ToStringArray()), ContinuationOptions);

                    tasks[i++] = pipeline.ListRangeAsync(_storage.GetRedisKey($"server:{serverName}:queues"))
                        .ContinueWith(x => queues.TryAdd(serverName, x.Result.ToStringArray()), ContinuationOptions);
                }

                pipeline.Execute();
                pipeline.WaitAll(tasks);

                return serverNames
                    .Select(serverName =>
                    {
                        var server = servers[serverName];

                        return new ServerDto
                        {
                            Name = serverName,
                            WorkersCount = int.Parse(server[0]),
                            Queues = queues[serverName],
                            StartedAt = JobHelper.DeserializeDateTime(server[1]),
                            Heartbeat = JobHelper.DeserializeNullableDateTime(server[2])
                        };
                    })
                    .ToList();
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var failedJobIds = redis
                    .SortedSetRangeByRank(_storage.GetRedisKey("failed"), from, from + count - 1)
					.ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new[] { "FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "State", "Reason" },
                    (job, jobData, state) => new FailedJobDto
                    {
                        Job = job,
                        Reason = state[5],
                        FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        ExceptionType = state[1],
                        ExceptionMessage = state[2],
                        ExceptionDetails = state[3],
                        InFailedState = FailedState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var succeededJobIds = redis
                    .ListRange(_storage.GetRedisKey("succeeded"), from, from + count - 1)
					.ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new[] { "SucceededAt", "PerformanceDuration", "Latency", "State", "Result" },
                    (job, jobData, state) => new SucceededJobDto
                    {
                        Job = job,
                        Result = state[4],
                        SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        TotalDuration = state[1] != null && state[2] != null
                            ? (long?) long.Parse(state[1]) + (long?) long.Parse(state[2])
                            : null,
                        InSucceededState = SucceededState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var deletedJobIds = redis
                    .ListRange(_storage.GetRedisKey("deleted"), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    deletedJobIds,
                    null,
                    new[] { "DeletedAt", "State" },
                    (job, jobData, state) => new DeletedJobDto
                    {
                        Job = job,
                        DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return UseConnection(redis =>
            {
                var queues = redis
                    .SetMembers(_storage.GetRedisKey("queues"))
					.ToStringArray();

                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Length);

                foreach (var queue in queues)
                {
                    string[] firstJobIds = null;
                    long length = 0;
                    long fetched = 0;
                    
					var pipeline = redis.CreateBatch();
					var tasks = new Task[3];
                    
					tasks[0] = pipeline.ListRangeAsync(_storage.GetRedisKey($"queue:{queue}"), -5, -1)
						.ContinueWith(x => firstJobIds = x.Result.ToStringArray(), ContinuationOptions);

                    tasks[1] = pipeline.ListLengthAsync(_storage.GetRedisKey($"queue:{queue}"))
						.ContinueWith(x => length = x.Result, ContinuationOptions);

                    tasks[2] = pipeline.ListLengthAsync(_storage.GetRedisKey($"queue:{queue}:dequeued"))
						.ContinueWith(x => fetched = x.Result, ContinuationOptions);

					pipeline.Execute();
					pipeline.WaitAll(tasks);

                    var jobs = GetJobsWithProperties(
                        redis,
                        firstJobIds,
                        new[] { "State" },
                        new[] { "EnqueuedAt", "State" },
                        (job, jobData, state) => new EnqueuedJobDto
                        {
                            Job = job,
                            State = jobData[0],
                            EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                            InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                        });

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Name = queue,
                        FirstJobs = jobs,
                        Length = length,
                        Fetched = fetched
                    });
                }

                return result;
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string queue, int from, int count)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis =>
            {
                var jobIds = redis
                    .ListRange(_storage.GetRedisKey($"queue:{queue}"), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State" },
                    new[] { "EnqueuedAt", "State" },
                    (job, jobData, state) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<FetchedJobDto> FetchedJobs([NotNull] string queue, int from, int count)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis =>
            {
                var jobIds = redis
                    .ListRange(_storage.GetRedisKey($"queue:{queue}:dequeued"), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State", "Fetched" },
                    null,
                    (job, jobData, state) => new FetchedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        FetchedAt = JobHelper.DeserializeNullableDateTime(jobData[1])
                    });
            });
        }

        public IDictionary<DateTime, long> HourlySucceededJobs() 
            => UseConnection(redis => GetHourlyTimelineStats(redis, "succeeded"));

        public IDictionary<DateTime, long> HourlyFailedJobs() 
            => UseConnection(redis => GetHourlyTimelineStats(redis, "failed"));

        public JobDetailsDto JobDetails([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            return UseConnection(redis =>
            {
                var entries = redis.HashGetAll(_storage.GetRedisKey($"job:{jobId}"));
                if (entries.Length == 0) return null;

                var jobData = entries.ToStringDictionary();
                
                var history = redis.ListRange(_storage.GetRedisKey($"job:{jobId}:history"));

                // history is in wrong order, fix this
                Array.Reverse(history);
                
                var stateHistory = new List<StateHistoryDto>(history.Length);
                foreach (var row in history)
                {
                    var entry = JobHelper.FromJson<Dictionary<string, string>>(row);
                    
                    var stateData = new Dictionary<string, string>(entry, StringComparer.OrdinalIgnoreCase);

                    stateHistory.Add(new StateHistoryDto
                    {
                        StateName = stateData.Pull("State", true),
                        Reason = stateData.Pull("Reason"),
                        CreatedAt = JobHelper.DeserializeDateTime(stateData.Pull("CreatedAt", true)),
                        Data = stateData
                    });
                }
                
                // some properties are not pulled,
                // but still need to be excluded
                jobData.Remove("State");
                jobData.Remove("Fetched");
                
                return new JobDetailsDto
                {
                    Job = TryToGetJob(
                        jobData.Pull("Type"), 
                        jobData.Pull("Method"),
                        jobData.Pull("ParameterTypes"), 
                        jobData.Pull("Arguments")),
                    CreatedAt = JobHelper.DeserializeNullableDateTime(jobData.Pull("CreatedAt")),
                    Properties = jobData,
                    History = stateHistory
                };
            });
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats([NotNull] IDatabase redis, [NotNull] string type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd-HH}")).ToArray();
            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out var value))
                {
                    value = 0;
                }

                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetTimelineStats([NotNull] IDatabase redis, [NotNull] string type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }
            
            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd}")).ToArray();

            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out var value))
                {
                    value = 0;
                }
                result.Add(dates[i], value);
            }

            return result;
        }

        private JobList<T> GetJobsWithProperties<T>(
            [NotNull] IDatabase redis,
            [NotNull] string[] jobIds,
            string[] properties,
            string[] stateProperties,
            [NotNull] Func<Job, IReadOnlyList<string>, IReadOnlyList<string>, T> selector)
        {
            if (jobIds == null) throw new ArgumentNullException(nameof(jobIds));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            if (jobIds.Length == 0)
            {
                return new JobList<T>(Enumerable.Empty<KeyValuePair<string, T>>());
            }
            
            var uniqueJobIds = jobIds.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            var jobProperties = new List<string> { "Type", "Method", "ParameterTypes", "Arguments" };
            var jobPropertiesOffset = 0;
            if (properties != null)
            {
                jobProperties.InsertRange(0, properties);
                jobPropertiesOffset = properties.Length;
            }
            
            var jobProps = jobProperties.ToRedisValues();
            var stateProps = stateProperties.ToRedisValues();

            var jobs = new ConcurrentDictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            var states = new ConcurrentDictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            
            var pipeline = redis.CreateBatch();
			var tasks = new Task[uniqueJobIds.Length * (stateProps != null ? 2 : 1)];

            var i = 0;
            foreach (var uniqueJobId in uniqueJobIds)
            {
                var jobId = uniqueJobId;
                
                tasks[i++] = pipeline.HashGetAsync(_storage.GetRedisKey($"job:{jobId}"), jobProps)
                    .ContinueWith(x => jobs.TryAdd(jobId, x.Result.ToStringArray()), ContinuationOptions);

				if (stateProps != null)
                {
                    tasks[i++] = pipeline.HashGetAsync(_storage.GetRedisKey($"job:{jobId}:state"), stateProps)
                        .ContinueWith(x => states.TryAdd(jobId, x.Result.ToStringArray()), ContinuationOptions);
				}
            }

            pipeline.Execute();
			pipeline.WaitAll(tasks);

            return new JobList<T>(jobIds
                .Select(jobId =>
                {
                    var job = jobs[jobId];
                    if (job.All(string.IsNullOrEmpty))
                        return new KeyValuePair<string, T>(jobId, default(T));

                    var method = TryToGetJob(job, jobPropertiesOffset);
                    var state = stateProps != null ? states[jobId] : null;
                    
                    return new KeyValuePair<string, T>(jobId, selector(method, job, state));
                }));
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(redis =>
            {
                var stats = new StatisticsDto();

                var queues = redis.SetMembers(_storage.GetRedisKey("queues"));

				var pipeline = redis.CreateBatch();
				var tasks = new Task[queues.Length + 8];

                tasks[0] = pipeline.SetLengthAsync(_storage.GetRedisKey("servers"))
					.ContinueWith(x=> stats.Servers = x.Result, ContinuationOptions);

                tasks[1] = pipeline.SetLengthAsync(_storage.GetRedisKey("queues"))
                    .ContinueWith(x => stats.Queues = x.Result, ContinuationOptions);

                tasks[2] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("schedule"))
					.ContinueWith(x => stats.Scheduled = x.Result, ContinuationOptions);

                tasks[3] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("processing"))
					.ContinueWith(x => stats.Processing = x.Result, ContinuationOptions);

                tasks[4] = pipeline.StringGetAsync(_storage.GetRedisKey("stats:succeeded"))
                    .ContinueWith(x => stats.Succeeded = long.Parse(x.Result.HasValue ?  (string)x.Result: "0"), ContinuationOptions);

                tasks[5] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("failed"))
					.ContinueWith(x => stats.Failed = x.Result, ContinuationOptions);

                tasks[6] = pipeline.StringGetAsync(_storage.GetRedisKey("stats:deleted"))
					.ContinueWith(x => stats.Deleted = long.Parse(x.Result.HasValue ?  (string)x.Result : "0"), ContinuationOptions);

                tasks[7] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("recurring-jobs"))
                    .ContinueWith(x => stats.Recurring = x.Result, ContinuationOptions);

                var enqueued = 0L;

				var i = 8;
                foreach (var queue in queues)
                {
                    tasks[i++] = pipeline.ListLengthAsync(_storage.GetRedisKey($"queue:{queue}"))
                        .ContinueWith(x => Interlocked.Add(ref enqueued, x.Result), ContinuationOptions);
                }

				pipeline.Execute();
				pipeline.WaitAll(tasks);

                stats.Enqueued = enqueued;
                return stats;
            });
        }

        private T UseConnection<T>(Func<IDatabase, T> action) => action(_database);

        private static Job TryToGetJob(string type, string method, string parameterTypes, string arguments)
        {
            try
            {
                return new InvocationData(
                    type,
                    method,
                    parameterTypes,
                    arguments).Deserialize();
            }
            catch (Exception)
            {
                return null;
            }
        }
        
        private static Job TryToGetJob(IReadOnlyList<string> values, int offset)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));
            if (offset < 0 || offset + 4 > values.Count)
                throw new ArgumentOutOfRangeException(nameof(offset));
            
            return TryToGetJob(values[offset], values[offset + 1], values[offset + 2], values[offset + 3]);
        }
    }
}
