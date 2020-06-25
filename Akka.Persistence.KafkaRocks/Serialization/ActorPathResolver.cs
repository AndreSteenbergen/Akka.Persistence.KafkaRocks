using MessagePack;
using MessagePack.Formatters;

namespace Akka.Persistence.KafkaRocks.Serialization
{
    public class ActorPathResolver : IFormatterResolver
    {
        public static IFormatterResolver Instance = new ActorPathResolver();
        private ActorPathResolver() { }
        public IMessagePackFormatter<T> GetFormatter<T>() => FormatterCache<T>.Formatter;

        private static class FormatterCache<T>
        {
            public static readonly IMessagePackFormatter<T> Formatter;
            static FormatterCache() => Formatter = (IMessagePackFormatter<T>)ActorPathResolverGetFormatterHelper.GetFormatter(typeof(T));
        }
    }
}