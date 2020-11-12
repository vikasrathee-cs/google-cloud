package io.cdap.plugin.gcp.publisher.source;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;
import scala.reflect.ClassTag;

public class PubSubInputDStream extends ReceiverInputDStream<ReceivedMessage> {

  protected String project;
  protected String topic;
  protected String subscription;
  protected ServiceAccountCredentials credentials;
  protected StorageLevel storageLevel;

  PubSubInputDStream(StreamingContext streamingContext, String project, String topic,
                     String subscription, ServiceAccountCredentials credentials, StorageLevel storageLevel) {
    super(streamingContext, ClassTag.<ReceivedMessage>apply(ReceivedMessage.class));
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.credentials = credentials;
    this.storageLevel = storageLevel;
  }

  @Override
  public Receiver<ReceivedMessage> getReceiver() {
    return new PubSubReceiver(this.project,
                              this.topic,
                              this.subscription,
                              this.credentials,
                              this.storageLevel,
                              this.rateController());
  }
}
