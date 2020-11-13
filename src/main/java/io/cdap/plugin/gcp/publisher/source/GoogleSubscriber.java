/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.gcp.publisher.source;

import com.google.pubsub.v1.ReceivedMessage;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;

/**
 * Realtime source plugin to read from Google PubSub.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("GoogleSubscriber")
@Description("Streaming Source to read messages from Google PubSub.")
public class GoogleSubscriber extends PubSubSubscriber<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSubscriber.class);
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING)))
    );

  private static final SerializableFunction<ReceivedMessage, StructuredRecord> MAPPING_FUNCTION =
    new SerializableFunction<ReceivedMessage, StructuredRecord>() {
      @Override
      public StructuredRecord apply(ReceivedMessage receivedMessage) {
        // Convert to a HashMap because com.google.api.client.util.ArrayMap is not serializable.
        HashMap<String, String> hashMap = new HashMap<>();
        if (receivedMessage.getMessage().getAttributesMap() != null) {
          hashMap.putAll(receivedMessage.getMessage().getAttributesMap());
        }

        return StructuredRecord.builder(DEFAULT_SCHEMA)
          .set("message", receivedMessage.getMessage().getData().toByteArray())
          .set("id", receivedMessage.getMessage().getMessageId())
          .setTimestamp("timestamp", getTimestamp(receivedMessage.getMessage().getPublishTime()))
          .set("attributes", hashMap)
          .build();
      }
    };

  protected static ZonedDateTime getTimestamp(com.google.protobuf.Timestamp timestamp) {
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    // Google cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to nanoseconds.
    // CDAP Schema only supports microsecond level precision so handle the case
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  public GoogleSubscriber(SubscriberConfig conf) {
    super(conf, DEFAULT_SCHEMA, MAPPING_FUNCTION);
    this.config = conf;
  }
}
