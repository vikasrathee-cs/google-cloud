package io.cdap.plugin.gcp.publisher.source;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class PubSubSubscriberTest {

  @Test
  public void testCreateSubscription() {
    PubSubReceiver receiver = Mockito.mock(PubSubReceiver.class);
    receiver.project = "project";
    receiver.topic = "topic";
    receiver.subscription = "subscription";
    receiver.backoffConfig = Mockito.mock(PubSubReceiver.BackoffConfig.class);
    when(receiver.backoffConfig.getInitialBackoffMs()).thenReturn(100);
  }
}
