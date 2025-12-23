using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Jitbit.Utils
{
    internal static class FastCacheStatics
    {
        internal static readonly SemaphoreSlim GlobalStaticLock =
            new SemaphoreSlim(1, 1);
    }

    public class FastCache<TKey, TValue> :
        IEnumerable<KeyValuePair<TKey, TValue>>, IDisposable
    {
        private readonly ConcurrentDictionary<TKey, TtlValue> _dict =
            new ConcurrentDictionary<TKey, TtlValue>();

        private readonly Timer _cleanUpTimer;
        private readonly EvictionCallback _itemEvicted;

        public delegate void EvictionCallback(TKey key);

        public FastCache(int cleanupJobInterval = 10000,
                         EvictionCallback itemEvicted = null)
        {
            _itemEvicted = itemEvicted;
            _cleanUpTimer = new Timer(
                s => EvictExpiredJob().ConfigureAwait(false),
                null,
                cleanupJobInterval,
                cleanupJobInterval);
        }

        private async Task EvictExpiredJob()
        {
            await FastCacheStatics.GlobalStaticLock
                .WaitAsync()
                .ConfigureAwait(false);

            try
            {
                EvictExpired();
            }
            finally
            {
                FastCacheStatics.GlobalStaticLock.Release();
            }
        }

        public void EvictExpired()
        {
            if (!Monitor.TryEnter(_cleanUpTimer))
                return;

            List<TKey> evictedKeys = null;

            try
            {
                long now = TimeUtil.NowMs();

                foreach (var p in _dict)
                {
                    if (p.Value.IsExpired(now))
                    {
                        TtlValue existing;
                        if (_dict.TryGetValue(p.Key, out existing) &&
                            ReferenceEquals(existing, p.Value) &&
                            _dict.TryRemove(p.Key, out existing))
                        {
                            if (_itemEvicted != null)
                            {
                                if (evictedKeys == null)
                                    evictedKeys = new List<TKey>();

                                evictedKeys.Add(p.Key);
                            }
                        }
                    }
                }
            }
            finally
            {
                Monitor.Exit(_cleanUpTimer);
            }

            OnEviction(evictedKeys);
        }

        public int Count
        {
            get { return _dict.Count; }
        }

        public void Clear()
        {
            _dict.Clear();
        }

        public void AddOrUpdate(TKey key, TValue value, TimeSpan ttl)
        {
            var ttlValue = new TtlValue(value, ttl);

            _dict.AddOrUpdate(
                key,
                k => ttlValue,
                (k, old) => ttlValue);
        }

        public void AddOrUpdate(
            TKey key,
            Func<TKey, TValue> addValueFactory,
            Func<TKey, TValue, TValue> updateValueFactory,
            TimeSpan ttl)
        {
            _dict.AddOrUpdate(
                key,
                k => new TtlValue(addValueFactory(k), ttl),
                (k, v) => new TtlValue(updateValueFactory(k, v.Value), ttl));
        }

        public bool TryGet(TKey key, out TValue value)
        {
            value = default(TValue);

            TtlValue ttlValue;
            if (!_dict.TryGetValue(key, out ttlValue))
                return false;

            if (ttlValue.IsExpired())
            {
                TtlValue existing;
                if (_dict.TryGetValue(key, out existing) &&
                    ReferenceEquals(existing, ttlValue))
                {
                    _dict.TryRemove(key, out existing);
                }

                OnEviction(key);
                return false;
            }

            value = ttlValue.Value;
            return true;
        }

        public bool TryAdd(TKey key, TValue value, TimeSpan ttl)
        {
            TValue dummy;
            if (TryGet(key, out dummy))
                return false;

            return _dict.TryAdd(key, new TtlValue(value, ttl));
        }

        private TValue GetOrAddCore(
            TKey key,
            Func<TValue> valueFactory,
            TimeSpan ttl)
        {
            bool wasAdded = false;

            var ttlValue = _dict.GetOrAdd(
                key,
                k =>
                {
                    wasAdded = true;
                    return new TtlValue(valueFactory(), ttl);
                });

            if (!wasAdded)
            {
                if (ttlValue.ModifyIfExpired(valueFactory, ttl))
                    OnEviction(key);
            }

            return ttlValue.Value;
        }

        public TValue GetOrAdd(
            TKey key,
            Func<TKey, TValue> valueFactory,
            TimeSpan ttl)
        {
            return GetOrAddCore(key, () => valueFactory(key), ttl);
        }

        public TValue GetOrAdd<TArg>(
            TKey key,
            Func<TKey, TArg, TValue> valueFactory,
            TimeSpan ttl,
            TArg arg)
        {
            return GetOrAddCore(key, () => valueFactory(key, arg), ttl);
        }

        public TValue GetOrAdd(
            TKey key,
            TValue value,
            TimeSpan ttl)
        {
            return GetOrAddCore(key, () => value, ttl);
        }

        public void Remove(TKey key)
        {
            TtlValue dummy;
            _dict.TryRemove(key, out dummy);
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            TtlValue ttlValue;
            bool res = _dict.TryRemove(key, out ttlValue) &&
                       !ttlValue.IsExpired();

            value = res ? ttlValue.Value : default(TValue);
            return res;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            long now = TimeUtil.NowMs();

            foreach (var kv in _dict)
            {
                if (!kv.Value.IsExpired(now))
                    yield return new KeyValuePair<TKey, TValue>(
                        kv.Key, kv.Value.Value);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private void OnEviction(TKey key)
        {
            if (_itemEvicted == null)
                return;

            Task.Run(() =>
            {
                try { _itemEvicted(key); }
                catch { }
            });
        }

        private void OnEviction(List<TKey> keys)
        {
            if (keys == null || keys.Count == 0 || _itemEvicted == null)
                return;

            Task.Run(() =>
            {
                try
                {
                    foreach (var key in keys)
                        _itemEvicted(key);
                }
                catch { }
            });
        }

        private sealed class TtlValue
        {
            public TValue Value { get; private set; }
            private long _expiresAt;

            public TtlValue(TValue value, TimeSpan ttl)
            {
                Value = value;
                _expiresAt = TimeUtil.NowMs() +
                             (long)ttl.TotalMilliseconds;
            }

            public bool IsExpired()
            {
                return IsExpired(TimeUtil.NowMs());
            }

            public bool IsExpired(long now)
            {
                return now > _expiresAt;
            }

            public bool ModifyIfExpired(
                Func<TValue> newValueFactory,
                TimeSpan newTtl)
            {
                long now = TimeUtil.NowMs();

                if (IsExpired(now))
                {
                    _expiresAt = now +
                                 (long)newTtl.TotalMilliseconds;
                    Value = newValueFactory();
                    return true;
                }

                return false;
            }
        }

        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
                _cleanUpTimer.Dispose();

            _disposed = true;
        }
    }

    internal static class TimeUtil
    {
        private static readonly double TickToMs =
            1000.0 / Stopwatch.Frequency;

        public static long NowMs()
        {
            return (long)(Stopwatch.GetTimestamp() * TickToMs);
        }
    }
}
