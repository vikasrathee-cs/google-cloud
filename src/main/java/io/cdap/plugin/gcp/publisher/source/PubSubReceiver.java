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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Spark Receiver for Pub/Sub Messages.
 * <p>
 * If backpressure is enabled, the message ingestion rate for this receiver will be managed by Spark.
 */
public class PubSubReceiver extends Receiver<ReceivedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubReceiver.class);

  protected BackoffConfig backoffConfig;
  protected String project;
  protected String topic;
  protected String subscription;
  protected boolean autoAcknowledge;
  protected ServiceAccountCredentials credentials;

  public PubSubReceiver(String project, @Nullable String topic, String subscription,
                        ServiceAccountCredentials credentials, boolean autoAcknowledge, StorageLevel storageLevel) {
    this(project, topic, subscription, credentials, autoAcknowledge, storageLevel,
         BackoffConfigBuilder.getInstance().build());
  }

  public PubSubReceiver(String project, @Nullable String topic, String subscription,
                        ServiceAccountCredentials credentials, boolean autoAcknowledge, StorageLevel storageLevel,
                        BackoffConfig backoffConfig) {
    super(storageLevel);

    this.backoffConfig = backoffConfig;
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.credentials = credentials;
    this.autoAcknowledge = autoAcknowledge;
  }

  @Override
  public void onStart() {
    if (topic != null) {
      createSubscription();
    }
    new Thread(this::receive).start();
  }

  @Override
  public void onStop() {
    //no-op
  }

  /**
   * Create a new subscription (if needed) for the specified topic.
   *
   * @throws IllegalArgumentException when the specified Topic does not exists
   * @throws RuntimeException         when the SubscriptionAdminClient cannot be created.
   * @throws ApiException             when a non-retryable exception is thrown by the Pub/Sub client
   */
  protected void createSubscription() {
    int backoff = backoffConfig.getInitialBackoffMs();
    int attempts = 5;

    ApiException lastApiException = null;

    while (!isStopped() && attempts-- > 0) {

      try (SubscriptionAdminClient subscriptionAdminClient = getSubscriptionAdminClient()) {

        ProjectTopicName topicName = ProjectTopicName.of(project, topic);
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subscription);
        int ackDeadline = 10; //10 seconds before resending the message.
        subscriptionAdminClient.createSubscription(
          subscriptionName, topicName, PushConfig.getDefaultInstance(), ackDeadline);
        return;

      } catch (ApiException ae) {

        lastApiException = ae;

        //If the subscription already exists, ignore the error.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.ALREADY_EXISTS)) {
          return;
        }

        //This error is thrown is the Topic Name is not valid.
        //Throw an Illegal Argument Exception so the pipeline fails.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.NOT_FOUND)) {
          String message = String.format("Failed to create subscription. Topic '%s' was not found in project '%s'.",
                                         topic,
                                         project);
          throw new IllegalArgumentException(message, ae);
        }

        //Retry if the exception is retriable.
        if (ae.isRetryable()) {
          backoff = sleepAndIncreaseBackoff(backoff);
          continue;
        }

        throw ae;
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create subscription.", ioe);
      }

    }

    if (isStopped()) {
      return;
    }

    throw new RuntimeException("Failed to create subscription after 5 attempts.", lastApiException);
  }

  /**
   * Receive new messages and store based on the Storage Level settings.
   *
   * @throws ApiException     when the Pub/Sub API throws a non-retryable exception.
   * @throws RuntimeException when the GrpcSubscriberStub cannot be created.
   */
  public void receive() {
    SubscriberStubSettings subscriberStubSettings = getSubscriberStubSettings();

    LOG.debug("Receiver Started execution");

    try (SubscriberStub subscriber = getSubscriberStub(subscriberStubSettings)) {
      String subscriptionName = ProjectSubscriptionName.format(project, subscription);
      fetchMessagesUntilStopped(subscriber, subscriptionName);
    } catch (IOException | ApiException e) {
      String message =
        String.format("Failed to fetch new messages using subscription '%s' for project '%s'.", subscription, project);
      throw new RuntimeException(message, e);
    }

    LOG.debug("Receiver completed execution");
  }

  /**
   * Fetch new messages for our subscription.
   * Implements exponential backoff strategy when a retryable exception is received.
   *
   * @param subscriber       The subscriber stub.
   * @param subscriptionName The name of the subscription to use to pull data.
   * @throws ApiException when the Pub/Sub API throws a non-retryable exception.
   */
  protected void fetchMessagesUntilStopped(SubscriberStub subscriber, String subscriptionName) {
    int backoff = backoffConfig.getInitialBackoffMs();

    while (!isStopped()) {
      try {
        fetchAndStoreMessages(subscriber, subscriptionName);
        backoff = backoffConfig.getInitialBackoffMs();
      } catch (ApiException ae) {
        if (ae.isRetryable()) {
          backoff = sleepAndIncreaseBackoff(backoff);
        } else {
          throw ae;
        }
      }
    }
  }

  /**
   * Fetch new messages, store in Spark's memory, and ack messages.
   * Based on SubscribeSyncExample.java in Google's PubSub examples.
   *
   * @param subscriber       The subscriber stub.
   * @param subscriptionName The name of the subscription to use to pull data.
   * @throws ApiException when the Pull request or ACK request fail.
   */
  protected void fetchAndStoreMessages(SubscriberStub subscriber, String subscriptionName) {
    PullRequest pullRequest =
      PullRequest.newBuilder()
        .setMaxMessages(getMessageRate())
        .setSubscription(subscriptionName)
        .build();
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

    List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

    //If there are no messages to process, continue.
    if (receivedMessages.isEmpty()) {
      return;
    }

    //Exit if the receiver is stopped before storing.
    if (isStopped()) {
      LOG.debug("Receiver stopped before store and ack.");
      return;
    }

    store(receivedMessages.iterator());

    if (autoAcknowledge) {
      List<String> ackIds =
        receivedMessages.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList());

      // Acknowledge received messages.
      AcknowledgeRequest acknowledgeRequest =
        AcknowledgeRequest.newBuilder()
          .setSubscription(subscriptionName)
          .addAllAckIds(ackIds)
          .build();
      subscriber.acknowledgeCallable().call(acknowledgeRequest);
    }
  }

  /**
   * Get Subscriber settings.
   *
   * @return the Subscriber Stub settings needed to subscribe to a Pub/Sub topic.
   */
  protected SubscriberStubSettings getSubscriberStubSettings() {
    try {
      return SubscriberStubSettings.newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
        .build();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to fetch messages. " +
                                   "Unable to create subscriber settings using the supplied credentials.", ioe);
    }
  }

  /**
   * Get Subscription Admin Client instance
   *
   * @return Subscription Admin Client instance.
   * @throws IOException
   */
  protected SubscriptionAdminClient getSubscriptionAdminClient() throws IOException {
    return SubscriptionAdminClient.create();
  }

  /**
   * Get Subscriber Stub instance
   *
   * @param subscriberStubSettings The settings for this Subscriber Stub client.
   * @return a new Subscriber Stub Instance
   * @throws IOException the exception thrown by the Pub/Sub library if the Subscriber Stub Settings are invalid.
   */
  protected SubscriberStub getSubscriberStub(SubscriberStubSettings subscriberStubSettings) throws IOException {
    return GrpcSubscriberStub.create(subscriberStubSettings);
  }

  /**
   * Sleep for a given number of milliseconds, calculate new backoff time and return.
   *
   * @param backoff the time in milliseconds to delay execution.
   * @return the new backoff delay in milliseconds
   */
  protected int sleepAndIncreaseBackoff(int backoff) {
    try {
      if (!isStopped()) {
        Thread.sleep(backoff);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted Exception in PubSubReceiver.");
    }

    return calculateUpdatedBackoff(backoff);
  }

  /**
   * Calculate the updated backoff period baded on the Backoff configuration parameters.
   *
   * @param backoff the previous backoff period
   * @return the updated backoff period for the next cycle.
   */
  protected int calculateUpdatedBackoff(int backoff) {
    return Math.min((int) (backoff * backoffConfig.getBackoffFactor()), backoffConfig.getMaximumBackoffMs());
  }

  /**
   * Get the rate at which this receiver should pull messages.
   * <p>
   * The default rate is 1000 (matching Apache Bahir) if the receiver has not been able to calculate a rate.
   *
   * @return The current rate at which this receiver should fetch messages.
   */
  protected int getMessageRate() {
    int messageRate = supervisor().getCurrentRateLimit() < (long) Integer.MAX_VALUE ?
      (int) supervisor().getCurrentRateLimit() : 1000;

    LOG.trace("Receiver rate is: {}", messageRate);

    return messageRate;
  }

  /**
   * Class used to configure exponential backoff for Pub/Sub API requests.
   */
  public static class BackoffConfig implements Serializable {
    final int initialBackoffMs;
    final int maximumBackoffMs;
    final double backoffFactor;

    public BackoffConfig(int initialBackoffMs, int maximumBackoffMs, double backoffFactor) {
      this.initialBackoffMs = initialBackoffMs;
      this.maximumBackoffMs = maximumBackoffMs;
      this.backoffFactor = backoffFactor;
    }

    public int getInitialBackoffMs() {
      return initialBackoffMs;
    }

    public int getMaximumBackoffMs() {
      return maximumBackoffMs;
    }

    public double getBackoffFactor() {
      return backoffFactor;
    }
  }

  /**
   * Builder class for BackoffConfig
   */
  public static class BackoffConfigBuilder implements Serializable {
    public int initialBackoffMs = 100;
    public int maximumBackoffMs = 10000;
    public double backoffFactor = 2.0;

    protected BackoffConfigBuilder() {
    }

    public static BackoffConfigBuilder getInstance() {
      return new BackoffConfigBuilder();
    }

    public BackoffConfig build() {
      if (initialBackoffMs > maximumBackoffMs) {
        throw new IllegalArgumentException("Maximum backoff cannot be smaller than Initial backoff");
      }

      return new BackoffConfig(initialBackoffMs, maximumBackoffMs, backoffFactor);
    }

    public int getInitialBackoffMs() {
      return initialBackoffMs;
    }

    public BackoffConfigBuilder setInitialBackoffMs(int initialBackoffMs) {
      this.initialBackoffMs = initialBackoffMs;
      return this;
    }

    public int getMaximumBackoffMs() {
      return maximumBackoffMs;
    }

    public BackoffConfigBuilder setMaximumBackoffMs(int maximumBackoffMs) {
      this.maximumBackoffMs = maximumBackoffMs;
      return this;
    }

    public double getBackoffFactor() {
      return backoffFactor;
    }

    public BackoffConfigBuilder setBackoffFactor(int backoffFactor) {
      this.backoffFactor = backoffFactor;
      return this;
    }
  }
}
