using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class KafkaQueueUriParser
    {
        internal const string Scheme = "kafka";

        public KafkaQueueUriParser(Uri uri)
        {
            Guard.AgainstNull(uri, "uri");

            if (!uri.Scheme.Equals(Scheme, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new InvalidSchemeException(Scheme, uri.ToString());
            }

            if (uri.LocalPath == "/" || uri.Segments.Length != 2)
            {
                throw new UriFormatException(string.Format(Esb.Resources.UriFormatException,
                    $"{Scheme}://{{configuration-name}}/{{topic}}", uri));
            }

            Uri = uri;

            ConfigurationName = Uri.Host;
            Topic = Uri.Segments[1];
        }

        public Uri Uri { get; }
        public string ConfigurationName { get; }
        public string Topic { get; }

    }
}