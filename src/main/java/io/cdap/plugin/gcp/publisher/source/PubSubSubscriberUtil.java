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

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Utility class to create a JavaDStream of received messages.
 */
public class PubSubSubscriberUtil implements Serializable {

  protected static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriberUtil.class);

  /**
   * Get a JavaDStream of received PubSubMessages.
   *
   * @param streamingContext the screaming context
   * @param config           The subscriver configuration
   * @return JavaDStream of all received pub/sub messages.
   * @throws Exception when the credentials could not be loaded.
   */
  public static JavaDStream<PubSubMessage> getStream(StreamingContext streamingContext,
                                                     PubSubSubscriberConfig config) throws Exception {
    SparkConf sparkConf = streamingContext.getSparkStreamingContext().ssc().conf();
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
    sparkConf.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true");
    sparkConf.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");

    ServiceAccountCredentials credentials = config.getServiceAccount() == null ?
      null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(),
                                                    config.isServiceAccountFilePath());
    boolean autoAcknowledge = true;
    if (streamingContext.isPreviewEnabled()) {
      autoAcknowledge = false;
    }

    JavaDStream<PubSubMessage> stream =
      getInputDStream(streamingContext, config, credentials, autoAcknowledge);

    return stream;
  }

  /**
   * Get a merged JavaDStream containing all received messages from multiple receivers.
   *
   * @param streamingContext the streaming context
   * @param config           subscriber config
   * @param credentials      GCP credentials
   * @param autoAcknowledge  if the messages should be acknowleged or not.
   * @return JavaDStream containing all received messages.
   */
  @SuppressWarnings("unchecked")
  protected static JavaDStream<PubSubMessage> getInputDStream(StreamingContext streamingContext,
                                                              PubSubSubscriberConfig config,
                                                              ServiceAccountCredentials credentials,
                                                              boolean autoAcknowledge) {
    ArrayList<JavaDStream<PubSubMessage>> receivers = new ArrayList<>(config.getNumberOfReceivers());
    ClassTag<PubSubMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class);

    for (int i = 1; i <= config.getNumberOfReceivers(); i++) {
      ReceiverInputDStream<PubSubMessage> receiverInputDStream =
        new PubSubInputDStream(streamingContext.getSparkStreamingContext().ssc(), config.getProject(),
                               config.getTopic(), config.getSubscription(), credentials, StorageLevel.MEMORY_ONLY(),
                               autoAcknowledge);
      receivers.add(new JavaReceiverInputDStream<>(receiverInputDStream, tag));
    }

    return (JavaDStream<PubSubMessage>) streamingContext.getSparkStreamingContext()
      .union(receivers.get(0), receivers.subList(1, receivers.size()));
  }

}
