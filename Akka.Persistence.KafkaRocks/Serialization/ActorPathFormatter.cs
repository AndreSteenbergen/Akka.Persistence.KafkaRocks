using Akka.Actor;
using MessagePack;
using MessagePack.Formatters;

namespace Akka.Persistence.KafkaRocks.Serialization
{
    public class ActorPathFormatter<T> : IMessagePackFormatter<T> where T : ActorPath
    {
        public void Serialize(ref MessagePackWriter writer, T value, MessagePackSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNil();
                return;
            }

            writer.Write(value.ToSerializationFormat());
        }

        public T Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            if (reader.TryReadNil())
            {
                return null;
            }
            
            options.Security.DepthStep(ref reader);
            var path = reader.ReadString();
            reader.Depth--;
            
            return ActorPath.TryParse(path, out var actorPath) ? (T)actorPath : null;
        }
    }
}