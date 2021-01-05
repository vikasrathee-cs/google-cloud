/*
 * Copyright © 2020 Cask Data, Inc.
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

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import javax.annotation.Nonnull;

/**
 * Spark Receiver for Pub/Sub Messages.
 * <p>
 * If backpressure is enabled, the message ingestion rate for this receiver will be managed by Spark.
 */
public class PubSubMessage implements Serializable {
  static final HashMap<String, String> EMPTY_MAP = new HashMap<>();

  String messageId;
  String orderingKey;
  String ackId;
  byte[] data;
  HashMap<String, String> attributes;
  Instant publishTime;

  public PubSubMessage(@Nonnull ReceivedMessage message) {
    if (message.getMessage() != null) {
      this.messageId = message.getMessage().getMessageId();
      this.orderingKey = message.getMessage().getOrderingKey();

      if (message.getMessage().getData() != null) {
        this.data = message.getMessage().getData().toByteArray();
      }

      if (message.getMessage().getAttributesMap() != null) {
        this.attributes = new HashMap<>(message.getMessage().getAttributesMap());
      }

      if (message.getMessage().getPublishTime() != null) {
        this.publishTime = Instant.ofEpochSecond(message.getMessage().getPublishTime().getSeconds())
          .plusNanos(message.getMessage().getPublishTime().getNanos());
      }
    }

    this.ackId = message.getAckId();
  }

  public String getMessageId() {
    return messageId;
  }

  public String getOrderingKey() {
    return orderingKey;
  }

  public String getAckId() {
    return ackId;
  }

  public byte[] getData() {
    return data;
  }

  public HashMap<String, String> getAttributes() {
    return attributes;
  }

  public Instant getPublishTime() {
    return publishTime;
  }
}
