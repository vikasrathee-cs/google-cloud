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
import org.apache.spark.streaming.scheduler.RateController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class PubSubReceiver extends Receiver<ReceivedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubReceiver.class);

  protected BackoffConfig backoffConfig;
  protected String project;
  protected String topic;
  protected String subscription;
  protected ServiceAccountCredentials credentials;
  protected Option<RateController> rateController;

  public PubSubReceiver(String project, String topic, String subscription, ServiceAccountCredentials credentials,
                        StorageLevel storageLevel, Option<RateController> rateController) {
    this(project, topic, subscription, credentials, storageLevel, rateController,
         BackoffConfigBuilder.getInstance().build());
  }

  public PubSubReceiver(String project, String topic, String subscription, ServiceAccountCredentials credentials,
                        StorageLevel storageLevel, Option<RateController> rateController, BackoffConfig backoffConfig) {
    super(storageLevel);

    this.backoffConfig = backoffConfig;
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.credentials = credentials;
    this.rateController = rateController;
  }

  @Override
  public void onStart() {
    createSubscription();
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

    while (!isStopped() && attempts-- > 0) {

      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {

        ProjectTopicName topicName = ProjectTopicName.of(this.project, this.topic);
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(this.project, this.subscription);
        int ackDeadline = 10; //10 seconds before resending the message.
        subscriptionAdminClient.createSubscription(
          subscriptionName, topicName, PushConfig.getDefaultInstance(), ackDeadline);
        return;

      } catch (ApiException ae) {

        //If the subscription already exists, ignore the error.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.ALREADY_EXISTS)) {
          return;
        }

        //This error is thrown is the Topic Name is not valid.
        //Throw an Illegal Argument Exception so the pipeline fails.
        if (ae.getStatusCode().getCode().equals(StatusCode.Code.NOT_FOUND)) {
          throw new IllegalArgumentException("Failed to create subscription. Topic Name is invalid.", ae);
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

    if (isStopped()) return;

    throw new RuntimeException("Failed to create subscription after 5 attempts.");
  }

  /**
   * Receive new messages and store based on the Storage Level settings.
   *
   * @throws ApiException     when the Pub/Sub API throws a non-retryable exception.
   * @throws RuntimeException when the GrpcSubscriberStub cannot be created.
   */
  public void receive() {
    SubscriberStubSettings subscriberStubSettings = getSubscriberStubSettings();

    try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
      String subscriptionName = ProjectSubscriptionName.format(this.project, this.subscription);
      fetchMessagesWithRetry(subscriber, subscriptionName);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to fetch new messages.", ioe);
    }
  }

  /**
   * Fetch new messages for our subscription.
   * Implements exponential backoff strategy when a retryable exception is received.
   *
   * @param subscriber       The subscriber stub.
   * @param subscriptionName The name of the subscription to use to pull data.
   * @throws ApiException when the Pub/Sub API throws a non-retryable exception.
   */
  protected void fetchMessagesWithRetry(SubscriberStub subscriber, String subscriptionName) {
    int backoff = backoffConfig.getInitialBackoffMs();

    while (!isStopped()) {
      try {
        fetchAndAck(subscriber, subscriptionName);
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
  protected void fetchAndAck(SubscriberStub subscriber, String subscriptionName) {
    int maxMessages = this.rateController.isDefined() ?
      this.rateController.map(RateController::getLatestRate).get().intValue() : 1000;

    PullRequest pullRequest =
      PullRequest.newBuilder()
        .setMaxMessages(maxMessages)
        .setSubscription(subscriptionName)
        .build();
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

    List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();
    List<String> ackIds = receivedMessages.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList());

    store(receivedMessages.iterator());

    // Acknowledge received messages.
    AcknowledgeRequest acknowledgeRequest =
      AcknowledgeRequest.newBuilder()
        .setSubscription(subscriptionName)
        .addAllAckIds(ackIds)
        .build();
    subscriber.acknowledgeCallable().call(acknowledgeRequest);
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
      throw new RuntimeException("Failed to fetch messages.", ioe);
    }
  }

  protected int sleepAndIncreaseBackoff(int backoff) {
    try {
      if (!isStopped()) {
        Thread.sleep(backoff);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted Exception in PubSubReceiver.");
    }
    backoff = Math.min((int) (backoff * backoffConfig.getBackoffFactor()), backoffConfig.getMaximumBackoffMs());

    return backoff;
  }

  public static class BackoffConfig {
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

  public static class BackoffConfigBuilder {
    public int initialBackoffMs = 100;
    public int maximumBackoffMs = 100;
    public double backoffFactor = 2.0;

    public BackoffConfigBuilder() {
    }

    public static BackoffConfigBuilder getInstance() {
      return new BackoffConfigBuilder();
    }

    public BackoffConfig build() {
      if (maximumBackoffMs > initialBackoffMs) {
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
