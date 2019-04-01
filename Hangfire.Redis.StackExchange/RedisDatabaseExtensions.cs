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

using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Hangfire.Redis
{
    public static class RedisDatabaseExtensions
    {
	    public static HashEntry[] ToHashEntries(this IEnumerable<KeyValuePair<string, string>> keyValuePairs)
	    {
		    if (keyValuePairs == null) return null;

		    if (keyValuePairs is ICollection<KeyValuePair<string, string>> collection)
			    return ToHashEntries(collection);
		    
		    return keyValuePairs.Select(kvp => new HashEntry(kvp.Key, kvp.Value)).ToArray();
	    }
	    
	    public static HashEntry[] ToHashEntries(this ICollection<KeyValuePair<string, string>> keyValuePairs)
	    {
		    if (keyValuePairs == null) return null;
		    if (keyValuePairs.Count == 0) return Array.Empty<HashEntry>();
		    
		    var array = new HashEntry[keyValuePairs.Count]; var i = 0;
		    foreach (var kvp in keyValuePairs)
			    array[i++] = new HashEntry(kvp.Key, kvp.Value);
		    
		    return array;
	    }

	    public static RedisValue[] ToRedisValues(this IList<string> list)
        {
	        if (list == null) return null;
	        if (list.Count == 0) return Array.Empty<RedisValue>();

            var array = new RedisValue[list.Count];
            for (var i = 0; i < array.Length; i++)
	            array[i] = (RedisValue)list[i];
            
            return array;
        }
        
        public static List<string> ToStringList(this RedisValue[] values)
        {
	        if (values == null) return null;
	        
	        var list = new List<string>(values.Length);
	        for (var i = 0; i < values.Length; i++)
		        list.Add((string)values[i]);
	        
	        return list;
        }

        public static T Pull<T>(this IDictionary<string, T> dictionary, string key, bool throwOnMissingKey = false)
        {
	        if (dictionary == null)
		        throw new ArgumentNullException(nameof(dictionary));
	        if (key == null)
		        throw new ArgumentNullException(nameof(key));

	        T value;
	        if (dictionary.TryGetValue(key, out value))
	        {
		        dictionary.Remove(key);
		        return value;
	        }

	        if (throwOnMissingKey)
		        throw new KeyNotFoundException($"Required value for key '{key}' is not found");

	        return default(T);
        }
        
        public static T Get<T>(this IDictionary<string, T> dictionary, string key, T fallback = default(T))
        {
	        if (dictionary == null)
		        throw new ArgumentNullException(nameof(dictionary));
	        if (key == null)
		        throw new ArgumentNullException(nameof(key));
	        
	        return dictionary.TryGetValue(key, out var value) ? value : fallback;
        }
        
		//public static Dictionary<string, string> ToStringDictionary(this HashEntry[] entries)
		//{
		//	var dictionary = new Dictionary<string, string>(entries.Length);
		//	foreach (var entry in entries)
		//		dictionary[entry.Name] = entry.Value;
		//	return dictionary;
		//}
		
        public static Dictionary<string, string> GetValuesMap(this IDatabase redis, string[] keys)
		{
			var redisKeyArr = keys.Select(x => (RedisKey)x).ToArray();
			var valuesArr = redis.StringGet(redisKeyArr);
			Dictionary<string, string> result = new Dictionary<string, string>(valuesArr.Length);
			for (int i = 0; i < valuesArr.Length; i++)
			{
				result.Add(redisKeyArr[i], valuesArr[i]);
			}
			return result;
		}


    }

}
