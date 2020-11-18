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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.pubsub.v1.ReceivedMessage;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Base implementation of a Realtime source plugin to read from Google PubSub.
 *
 * @param <T> The type that the ReceivedMessage will be mapped to by the mapping function.
 */
public abstract class PubSubSubscriber<T> extends StreamingSource<T> {

  protected static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriber.class);

  protected SubscriberConfig config;
  protected Schema schema;
  protected SerializableFunction<ReceivedMessage, T> mappingFunction;

  public PubSubSubscriber(SubscriberConfig conf, Schema schema,
                          SerializableFunction<ReceivedMessage, T> mappingFunction) {
    this.config = conf;
    this.schema = schema;
    this.mappingFunction = mappingFunction;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(this.schema);
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    // record dataset lineage
    context.registerLineage(config.referenceName, this.schema);

    if (this.schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from Pub/Sub",
                          this.schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<T> getStream(StreamingContext streamingContext) throws Exception {
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

    JavaReceiverInputDStream<ReceivedMessage> stream =
      getInputDStream(streamingContext, credentials, autoAcknowledge);

    return (JavaDStream<T>) stream.map(pubSubMessage -> mappingFunction.apply(pubSubMessage));
  }

  protected JavaReceiverInputDStream<ReceivedMessage> getInputDStream(StreamingContext streamingContext,
                                                                      ServiceAccountCredentials credentials,
                                                                      boolean autoAcknowledge) {
    ReceiverInputDStream<ReceivedMessage> stream =
      new PubSubInputDStream(streamingContext.getSparkStreamingContext().ssc(), config.getProject(),
                             config.getTopic(), config.getSubscription(), credentials, StorageLevel.MEMORY_ONLY(),
                             autoAcknowledge);
    ClassTag<ReceivedMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(ReceivedMessage.class);
    return new JavaReceiverInputDStream<>(stream, tag);
  }

  @Override
  public int getRequiredExecutors() {
    return 1;
  }

  /**
   * Configuration class for the subscriber source.
   */
  public static class SubscriberConfig extends GCPReferenceSourceConfig {

    @Description("Cloud Pub/Sub subscription to read from. If a subscription with the specified name does not " +
      "exist, it will be automatically created if a topic is specified. Messages published before the subscription " +
      "was created will not be read.")
    @Macro
    private String subscription;

    @Description("Cloud Pub/Sub topic to create a subscription on. This is only used when the specified  " +
      "subscription does not already exist and needs to be automatically created. If the specified " +
      "subscription already exists, this value is ignored.")
    @Macro
    @Nullable
    private String topic;

    @Description("Set the number of receivers to run in parallel. There need to be enough workers in the " +
      "cluster to run all receivers. By default, only 1 receiver is running per Pub/Sub Source.")
    @Macro
    @Nullable
    private Integer numberOfReceivers;

    public void validate(FailureCollector collector) {
      super.validate(collector);
      String regAllowedChars = "[A-Za-z0-9-.%~+_]*$";
      String regStartWithLetter = "[A-Za-z]";
      if (!getSubscription().matches(regAllowedChars)) {
        collector.addFailure("Subscription Name does not match naming convention.",
                             "Unexpected Character. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (getSubscription().startsWith("goog")) {
        collector.addFailure("Subscription Name does not match naming convention.",
                             " Cannot Start with String goog. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (!getSubscription().substring(0, 1).matches(regStartWithLetter)) {
        collector.addFailure("Subscription Name does not match naming convention.",
                             "Name must start with a letter. Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      if (getSubscription().length() < 3 || getSubscription().length() > 255) {
        collector.addFailure("Subscription Name does not match naming convention.",
                             "Character Length must be between 3 and 255 characters. " +
                               "Check Plugin documentation for naming convention.")
          .withConfigProperty(subscription);
      }
      collector.getOrThrowException();
    }

    public String getSubscription() {
      return subscription;
    }

    public String getTopic() {
      return topic;
    }

    public int getNumberOfReceivers() {
      return numberOfReceivers;
    }
  }
}
