using System;
using System.Collections.Generic;
using Akka.Actor;

namespace Akka.Persistence.KafkaRocks.Serialization
{
    internal static class ActorPathResolverGetFormatterHelper
    {
        private static readonly Dictionary<Type, object> FormatterMap = new Dictionary<Type, object>
        {
            {typeof(ActorPath), new ActorPathFormatter<ActorPath>()},
            {typeof(ChildActorPath), new ActorPathFormatter<ChildActorPath>()},
            {typeof(RootActorPath), new ActorPathFormatter<RootActorPath>()}
        };

        internal static object GetFormatter(Type t) => FormatterMap.TryGetValue(t, out var formatter) ? formatter : null;
    }
}