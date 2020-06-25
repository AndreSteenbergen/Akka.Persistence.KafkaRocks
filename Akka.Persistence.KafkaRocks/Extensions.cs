using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using RocksDbSharp;

namespace Akka.Persistence.KafkaRocks
{
    internal static class CollectionsExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AddBinding<TKey, TVal>(this Dictionary<TKey, HashSet<TVal>> dictionary, TKey key, TVal item)
        {
            if (!dictionary.TryGetValue(key, out HashSet<TVal> bucket))
            {
                dictionary[key] = bucket = new HashSet<TVal>();
            }

            bucket.Add(item);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void RemoveBinding<TKey, TVal>(this Dictionary<TKey, HashSet<TVal>> dictionary, TKey key, TVal item)
        {
            if (!dictionary.TryGetValue(key, out HashSet<TVal> bucket)) return;
            if (bucket.Remove(item) && bucket.Count == 0)
            {
                dictionary.Remove(key);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> list)
        {
            return new HashSet<T>(list);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (byte[] Key, byte[] Value) Peek(this Iterator iterator)
        {
            if (!iterator.Valid())
                throw new InvalidOperationException();

            return (iterator.Key(), iterator.Value());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (byte[] Key, byte[] Value) PeekAndNext(this Iterator iterator)
        {
            var data = iterator.Peek();
            iterator.Next();
            return data;
        }
    }
}