package io.cdap.plugin.gcp.publisher.source;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;

import javax.annotation.Nullable;

/**
 * Input Stream used to subscribe to a Pub/Sub topic and pull messages.
 */
public class PubSubInputDStream extends ReceiverInputDStream<ReceivedMessage> {

  protected String project;
  protected String topic;
  protected String subscription;
  protected ServiceAccountCredentials credentials;
  protected StorageLevel storageLevel;
  protected boolean autoAcknowledge;

  /**
   * Constructor Method
   *
   * @param streamingContext Spark Streaming Context
   * @param project          Project Name
   * @param topic            Topic Name
   * @param subscription     Subscription Name
   * @param credentials      Google Cloud credentials
   * @param storageLevel     Spark Storage Level for received messages
   * @param autoAcknowledge  Acknowledge messages
   */
  PubSubInputDStream(StreamingContext streamingContext, String project, @Nullable String topic,
                     String subscription, ServiceAccountCredentials credentials, StorageLevel storageLevel,
                     boolean autoAcknowledge) {
    super(streamingContext, scala.reflect.ClassTag$.MODULE$.apply(ReceivedMessage.class));
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.credentials = credentials;
    this.storageLevel = storageLevel;
    this.autoAcknowledge = autoAcknowledge;
  }



  @Override
  public Receiver<ReceivedMessage> getReceiver() {
    return new PubSubReceiver(this.project,
                              this.topic,
                              this.subscription,
                              this.credentials,
                              this.autoAcknowledge,
                              this.storageLevel);
  }
}
