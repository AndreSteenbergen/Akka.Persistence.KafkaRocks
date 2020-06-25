using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Akka.Persistence.KafkaRocks.Query
{
    internal class DeliveryBuffer<T>
    {
        public ImmutableArray<T> Buffer { get; private set; } = ImmutableArray<T>.Empty;
        public bool IsEmpty => Buffer.IsEmpty;
        public int Length => Buffer.Length;

        private readonly Action<T> onNext;

        public DeliveryBuffer(Action<T> onNext)
        {
            this.onNext = onNext;
        }

        public void Add(T element)
        {
            Buffer = Buffer.Add(element);
        }
        public void AddRange(IEnumerable<T> elements)
        {
            Buffer = Buffer.AddRange(elements);
        }

        public void DeliverBuffer(long demand)
        {
            if (!Buffer.IsEmpty && demand > 0)
            {
                var totalDemand = Math.Min((int)demand, Buffer.Length);
                if (Buffer.Length == 1)
                {
                    // optimize for this common case
                    onNext(Buffer[0]);
                    Buffer = ImmutableArray<T>.Empty;
                }
                else if (demand <= int.MaxValue)
                {
                    for (var i = 0; i < totalDemand; i++)
                        onNext(Buffer[i]);

                    Buffer = Buffer.RemoveRange(0, totalDemand);
                }
                else
                {
                    foreach (var element in Buffer)
                        onNext(element);

                    Buffer = ImmutableArray<T>.Empty;
                }
            }
        }

    }
}