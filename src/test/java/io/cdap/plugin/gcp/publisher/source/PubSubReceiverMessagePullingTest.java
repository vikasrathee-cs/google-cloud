package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SubscriptionAdminClient.class)
public class PubSubReceiverMessagePullingTest {

  @Mock
  StatusCode statusCode;

  @Mock
  ApiException apiException;

  @Mock
  SubscriberStubSettings subscriberStubSettings;

  @Mock
  SubscriberStub subscriberStub;

  @Mock
  PubSubReceiver receiver;

  @Before
  public void setup() throws IOException {
    when(receiver.getSubscriberStubSettings()).thenReturn(subscriberStubSettings);
    when(receiver.getSubscriberStub(any())).thenReturn(subscriberStub);

    receiver.project = "my-project";
    receiver.topic = "my-topic";
    receiver.subscription = "my-subscription";
    receiver.backoffConfig = mock(PubSubReceiver.BackoffConfig.class);

    when(receiver.backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(receiver.backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(receiver.backoffConfig.getMaximumBackoffMs()).thenReturn(10000);
  }

  @Test
  public void testReceiveSuccessCase() throws IOException {
    doCallRealMethod().when(receiver).receive();

    receiver.receive();

    verify(receiver, times(1))
      .fetchMessagesWithRetry(subscriberStub, "projects/my-project/subscriptions/my-subscription");
  }

  @Test(expected = RuntimeException.class)
  public void testReceiveIOException() throws IOException {
    doThrow(new IOException("Some exception")).when(receiver).receive();

    receiver.receive();
  }

}
