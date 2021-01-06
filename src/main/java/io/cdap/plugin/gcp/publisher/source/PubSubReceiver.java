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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.TopicName;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Spark Receiver for Pub/Sub Messages.
 * <p>
 * If backpressure is enabled, the message ingestion rate for this receiver will be managed by Spark.
 */
public class PubSubReceiver extends Receiver<PubSubMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubReceiver.class);
  private static final String CREATE_SUBSCRIPTION_ERROR_MSG =
    "Failed to create subscription '%s'.";
  private static final String CREATE_SUBSCRIPTION_ADMIN_CLIENT_ERROR_MSG =
    "Failed to create subscription client to manage subscription '%s'.";
  private static final String CREATE_SUBSCRIPTION_RETRY_ERROR_MSG =
    "Failed to create subscription '%s' after 5 attempts";
  private static final String MISSING_TOPIC_ERROR_MSG =
    "Failed to create subscription. Topic '%s' was not found in project '%s'.";
  private static final String SUBSCRIBER_ERROR_MSG =
    "Failed to create subscriber using subscription '%s' for project '%s'.";
  private static final String FETCH_ERROR_MSG =
    "Failed to fetch new messages using subscription '%s' for project '%s'.";

  private PubSubSubscriberConfig config;
  private boolean autoAcknowledge;
  private BackoffConfig backoffConfig;
  private int previousFetchRate = -1;

  //Transient properties used by the receiver in the worker node.
  private transient String project;
  private transient String topic;
  private transient String subscription;
  private transient Credentials credentials;
  private transient ScheduledThreadPoolExecutor executor;
  private transient SubscriberStub subscriber;
  private transient AtomicInteger bucket;

  public PubSubReceiver(PubSubSubscriberConfig config, boolean autoAcknowledge, StorageLevel storageLevel) {
    this(config, autoAcknowledge, storageLevel, BackoffConfig.defaultInstance());
  }

  public PubSubReceiver(PubSubSubscriberConfig config, boolean autoAcknowledge, StorageLevel storageLevel,
                        BackoffConfig backoffConfig) {
    super(storageLevel);

    this.config = config;
    this.autoAcknowledge = autoAcknowledge;
    this.backoffConfig = backoffConfig;
  }

  @VisibleForTesting
  public PubSubReceiver(String project, String topic, String subscription, Credentials credentials,
                        boolean autoAcknowledge, StorageLevel storageLevel, BackoffConfig backoffConfig,
                        ScheduledThreadPoolExecutor executor, SubscriberStub subscriber, AtomicInteger bucket) {
    super(storageLevel);
    this.backoffConfig = backoffConfig;
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.autoAcknowledge = autoAcknowledge;
    this.credentials = credentials;
    this.executor = executor;
    this.subscriber = subscriber;
    this.bucket = bucket;
  }

  @Override
  public void onStart() {
    //Configure Executor Service
    this.executor = new ScheduledThreadPoolExecutor(3, new LoggingRejectedExecutionHandler());
    this.executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    this.executor.setRemoveOnCancelPolicy(true);

    //Create counter used to restrict the number of messages we fetch every second.
    this.bucket = new AtomicInteger();

    //Configure properties
    this.project = config.getProject();
    this.topic = TopicName.format(config.getProject(), config.getTopic());
    this.subscription = ProjectSubscriptionName.format(config.getProject(), config.getSubscription());

    try {
      this.credentials = config.getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(),
                                                      config.isServiceAccountFilePath());
    } catch (IOException e) {
      LOG.error("Unable to get credentials.");
      stop("Unable to get credentials for receiver.", e);
    }

    //Create subscription if the topic is specified.
    if (topic != null) {
      createSubscription();
    }

    //Schedule tasks to set the message rate (Token Bucket algorithm) and start the receiver worker.
    this.executor.scheduleAtFixedRate(this::updateMessageRateAndFillBucket, 0, 1, TimeUnit.SECONDS);
    this.executor.submit(this::scheduleFetch);

    LOG.info("Receiver started execution");
  }

  @Override
  public void onStop() {
    //Shutdown thread pool executor
    if (executor != null && !executor.isShutdown()) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while waiting for executor to shutdown.");
      }
    }

    //Clean up subscriber stub used by the Google Cloud client.
    if (subscriber != null && !subscriber.isShutdown()) {
      subscriber.shutdown();
      try {
        subscriber.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while waiting for subscriber to shutdown.");
      }
    }

    LOG.info("Receiver completed execution");
  }

  /**
   * Create a new subscription (if needed) for the supplied topic.
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

        int ackDeadline = 10; //10 seconds before resending the message.
        subscriptionAdminClient.createSubscription(
          subscription, topic, PushConfig.getDefaultInstance(), ackDeadline);
        return;

      } catch (ApiException ae) {

        lastApiException = ae;

        //If the subscription already exists, ignore the error.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.ALREADY_EXISTS)) {
          return;
        }

        //This error is thrown is the Topic does not exist.
        // Call the stop method so the pipeline fails.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.NOT_FOUND)) {
          String message = String.format(MISSING_TOPIC_ERROR_MSG, topic, project);
          stop(message, ae);
          return;
        }

        //Retry if the exception is retriable.
        if (ae.isRetryable()) {
          backoff = sleepAndIncreaseBackoff(backoff);
          continue;
        }

        //Report that we were not able to create the subscription and stop the receiver.
        stop(String.format(CREATE_SUBSCRIPTION_ERROR_MSG, subscription), ae);
        return;
      } catch (IOException ioe) {
        //Report that we were not able to create the subscription admin client and stop the receiver.
        stop(String.format(CREATE_SUBSCRIPTION_ADMIN_CLIENT_ERROR_MSG, subscription), ioe);
        return;
      }
    }

    //If we were not able to create the subscription after 5 attempts, stop the pipeline and report the error.
    stop(String.format(CREATE_SUBSCRIPTION_RETRY_ERROR_MSG, subscription), lastApiException);
  }

  /**
   * Build subscriber client and schedule fetch task to run at a 100 millisecond delay.
   *
   * @throws ApiException     when the Pub/Sub API throws a non-retryable exception.
   * @throws RuntimeException when the GrpcSubscriberStub cannot be created.
   */
  public void scheduleFetch() {
    try {
      SubscriberStubSettings subscriberSettings = getSubscriberSettings();
      subscriber = getSubscriber(subscriberSettings);

      executor.scheduleWithFixedDelay(this::receiveMessages, 0, 100, TimeUnit.MILLISECONDS);
    } catch (IOException ioe) {
      //This exception is thrown when the subscriber could not be created.
      //Report the exception and stop the receiver.
      String message =
        String.format(SUBSCRIBER_ERROR_MSG, subscription, project);
      stop(message, ioe);
    }
  }

  /**
   * Fetch new messages for our subscription.
   * Implements exponential backoff strategy when a retryable exception is received.
   * This method stops the receiver if a non retryable ApiException is thrown by the Google Cloud subscriber client.
   */
  protected void receiveMessages() {
    int backoff = backoffConfig.getInitialBackoffMs();

    //Try with backoff until stopped or the task succeeds.
    while (!isStopped()) {
      try {
        fetchAndAck();
        return;
      } catch (ApiException ae) {
        if (ae.isRetryable()) {
          backoff = sleepAndIncreaseBackoff(backoff);
        } else {
          //Restart the receiver if the exception is not retryable.
          String message =
            String.format(FETCH_ERROR_MSG, subscription, project);
          restart(message, ae);
          break;
        }
      }
    }
  }

  /**
   * Fetch new messages, store in Spark's memory, and ack messages.
   * Based on SubscribeSyncExample.java in Google's PubSub examples.
   */
  protected void fetchAndAck() {
    //Get the maximun number of messages to get. If this number is less or equal than 0, do not fetch.
    int maxMessages = bucket.get();
    if (maxMessages <= 0) {
      return;
    }

    PullRequest pullRequest =
      PullRequest.newBuilder()
        .setMaxMessages(maxMessages)
        .setSubscription(subscription)
        .build();
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

    List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

    //If there are no messages to process, continue.
    if (receivedMessages.isEmpty()) {
      return;
    }

    //Decrement number of available messages in bucket.
    bucket.updateAndGet(x -> x - receivedMessages.size());

    //Exit if the receiver is stopped before storing.
    if (isStopped()) {
      LOG.debug("Receiver stopped before store and ack.");
      return;
    }

    List<PubSubMessage> messages = receivedMessages.stream().map(PubSubMessage::new).collect(Collectors.toList());

    store(messages.iterator());

    if (autoAcknowledge) {
      List<String> ackIds =
        messages.stream().map(PubSubMessage::getAckId).collect(Collectors.toList());

      // Acknowledge received messages.
      AcknowledgeRequest acknowledgeRequest =
        AcknowledgeRequest.newBuilder()
          .setSubscription(subscription)
          .addAllAckIds(ackIds)
          .build();
      subscriber.acknowledgeCallable().call(acknowledgeRequest);
    }
  }

  /**
   * Get Subscriber settings instance.
   *
   * @return the Subscriber Stub settings needed to subscribe to a Pub/Sub topic.
   */
  protected SubscriberStubSettings getSubscriberSettings() throws IOException {
    SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();

    if (credentials != null) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }

    return builder.build();
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
  protected SubscriberStub getSubscriber(SubscriberStubSettings subscriberStubSettings) throws IOException {
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
        LOG.debug("Backoff - Sleeping for {} ms.", backoff);
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
   * Get the rate at which this receiver should pull messages and set this rate in the bucket we use for rate control.
   * The default rate is Integer.MAX_VALUE if the receiver has not been able to calculate a rate.
   */
  protected void updateMessageRateAndFillBucket() {
    int messageRate = (int) Math.min(supervisor().getCurrentRateLimit(), Integer.MAX_VALUE);

    if (messageRate != previousFetchRate) {
      previousFetchRate = messageRate;
      LOG.debug("Receiver fetch rate is set to: {}", messageRate);
    }

    bucket.set(messageRate);
  }

  /**
   * Class used to configure exponential backoff for Pub/Sub API requests.
   */
  public static class BackoffConfig implements Serializable {
    final int initialBackoffMs;
    final int maximumBackoffMs;
    final double backoffFactor;

    static final BackoffConfig defaultInstance() {
      return new BackoffConfig(100, 10000, 2.0);
    }

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

  /**
   * Rejected execution handler which logs a message when a task is rejected.
   */
  protected static class LoggingRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      LOG.error("Thread Pool rejected execution of a task.");
    }
  }
}
