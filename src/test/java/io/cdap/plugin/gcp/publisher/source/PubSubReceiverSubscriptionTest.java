package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
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
public class PubSubReceiverSubscriptionTest {

  @Mock
  StatusCode statusCode;

  @Mock
  ApiException apiException;

  @Mock
  SubscriptionAdminClient subscriptionAdminClient;

  @Mock
  PubSubReceiver receiver;

  @Before
  public void setUp() {
    receiver.project = "project";
    receiver.topic = "topic";
    receiver.subscription = "subscription";
    receiver.backoffConfig = mock(PubSubReceiver.BackoffConfig.class);

    when(receiver.backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(receiver.backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(receiver.backoffConfig.getMaximumBackoffMs()).thenReturn(10000);
  }

  @Test
  public void testCreateSubscriptionSuccessCase() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).getSubscriptionAdminClient();

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(1))
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          eq(10));
  }

  @Test
  public void testCreateSubscriptionAlreadyExistsCase() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).getSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);

    doThrow(apiException)
      .when(subscriptionAdminClient)
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          anyInt());

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(1))
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          eq(10));
    verify(apiException, times(1))
      .getStatusCode();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSubscriptionTopicDoesNotExist() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).getSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);

    doThrow(apiException)
      .when(subscriptionAdminClient)
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          anyInt());

    receiver.createSubscription();
  }

  @Test(expected = ApiException.class)
  public void testCreateSubscriptionAnyOtherAPIException() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).getSubscriptionAdminClient();
    // We need to throw a proper instance for ApiException and not a mock in this case.
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ABORTED);

    doThrow(apiException)
      .when(subscriptionAdminClient)
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          anyInt());

    receiver.createSubscription();
  }

  @Test(expected = RuntimeException.class)
  public void testCreateSubscriptionWrapIOExceptionInRuntimeException() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doThrow(new IOException()).when(receiver).getSubscriptionAdminClient();

    receiver.createSubscription();
  }

  @Test
  public void testSubscriptionRetryLogic() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).getSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    when(apiException.isRetryable()).thenReturn(true);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ABORTED);

    when(subscriptionAdminClient.createSubscription(any(ProjectSubscriptionName.class),
                                                    any(ProjectTopicName.class),
                                                    any(PushConfig.class),
                                                    anyInt()))
      .thenThrow(apiException)
      .thenThrow(apiException)
      .thenReturn(Subscription.newBuilder().build());

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(3))
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          eq(10));
    verify(apiException, times(4)) // 2 checks on each loop
      .getStatusCode();
  }

  @Test(expected = RuntimeException.class)
  public void testSubscriptionRetryLogicExceedsAttempts() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).getSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    when(apiException.isRetryable()).thenReturn(true);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ABORTED);

    when(subscriptionAdminClient.createSubscription(any(ProjectSubscriptionName.class),
                                                    any(ProjectTopicName.class),
                                                    any(PushConfig.class),
                                                    anyInt()))
      .thenThrow(apiException)
      .thenThrow(apiException)
      .thenThrow(apiException)
      .thenThrow(apiException)
      .thenThrow(apiException)
      .thenThrow(apiException);

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(5))
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          eq(10));
  }

  @Test
  public void testSubscriptionIsStopped() {
    when(receiver.isStopped()).thenReturn(true);

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(0))
      .createSubscription(any(ProjectSubscriptionName.class),
                          any(ProjectTopicName.class),
                          any(PushConfig.class),
                          eq(10));
  }

}
