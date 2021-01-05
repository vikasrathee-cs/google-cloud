package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnaryCallable.class, PullResponse.class, GeneratedMessageV3.class, ReceivedMessage.class})
public class PubSubReceiverMessagePullingTest {

  Credentials credentials = null;
  boolean autoAcknowledge = false;
  StorageLevel level = StorageLevels.MEMORY_ONLY;
  AtomicInteger tokenBucket = new AtomicInteger();

  @Mock
  StatusCode statusCode;

  @Mock
  ApiException apiException;

  @Mock
  SubscriberStubSettings subscriberSettings;

  @Mock
  SubscriberStub subscriber;

  PubSubReceiver receiver;

  @Mock
  UnaryCallable<PullRequest, PullResponse> pullCallable;

  @Mock
  UnaryCallable<AcknowledgeRequest, Empty> acknowledgeCallable;

  @Mock
  PubSubReceiver.BackoffConfig backoffConfig;

  @Mock
  ScheduledThreadPoolExecutor executor;

  @Captor
  ArgumentCaptor<PullRequest> pullRequestArgumentCaptor;

  @Captor
  ArgumentCaptor<AcknowledgeRequest> acknowledgeRequestArgumentCaptor;

  @Before
  public void setup() throws IOException {
    receiver = new PubSubReceiver("my-project", "my-topic", "my-subscription", credentials,
                                  autoAcknowledge, level, backoffConfig, executor, subscriber, tokenBucket);
    receiver = spy(receiver);

    when(receiver.getSubscriberSettings()).thenReturn(subscriberSettings);
    when(receiver.getSubscriber(any())).thenReturn(subscriber);
    when(receiver.getAndUpdateMessageRate()).thenReturn(12345);

    when(backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(10000);

    when(subscriber.pullCallable()).thenReturn(pullCallable);
    when(subscriber.acknowledgeCallable()).thenReturn(acknowledgeCallable);
  }

  public ReceivedMessage getReceivedMessage(String ackId) {
    ReceivedMessage message = ReceivedMessage.newBuilder().setAckId(ackId).buildPartial();
    return message;
  }

  @Test
  public void testReceiveSuccessCase() throws IOException {
    doCallRealMethod().when(receiver).receive();

    receiver.receive();

    verify(receiver, times(1))
      .fetchWithBackoff(subscriber, "projects/my-project/subscriptions/my-subscription");
  }

  @Test(expected = RuntimeException.class)
  public void testReceiveIOException() throws IOException {
    doThrow(new IOException("Some exception")).when(receiver).receive();

    receiver.receive();
  }

  @Test(expected = RuntimeException.class)
  public void testReceiveApiException() throws IOException {
    doThrow(apiException).when(receiver).receive();

    receiver.receive();
  }

  @Test
  public void testFetchAckWithRetry() throws IOException {
    doCallRealMethod().when(receiver).fetchWithBackoff(any(), anyString());

    //Stop after 3 iterations
    when(receiver.isStopped())
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(true);

    receiver.fetchWithBackoff(subscriber, "some-sub");

    verify(receiver, times(3)).fetchAndStoreMessages(eq(subscriber), eq("some-sub"));
  }

  @Test
  public void testFetchAckWithRetryBackoff() throws IOException {
    doCallRealMethod().when(receiver).fetchWithBackoff(any(), anyString());
    when(apiException.isRetryable()).thenReturn(true);

    doThrow(apiException).when(receiver).fetchAndStoreMessages(any(), anyString());

    when(receiver.sleepAndIncreaseBackoff(anyInt()))
      .thenReturn(200)
      .thenReturn(400)
      .thenReturn(800)
      .thenReturn(1600)
      .thenReturn(3200);

    //Stop after 3 iterations
    when(receiver.isStopped())
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(true);

    receiver.fetchWithBackoff(subscriber, "some-sub");

    verify(receiver, times(5)).fetchAndStoreMessages(eq(subscriber), eq("some-sub"));
    verify(receiver, times(5)).sleepAndIncreaseBackoff(anyInt());
    verify(receiver).sleepAndIncreaseBackoff(100);
    verify(receiver).sleepAndIncreaseBackoff(200);
    verify(receiver).sleepAndIncreaseBackoff(400);
    verify(receiver).sleepAndIncreaseBackoff(800);
    verify(receiver).sleepAndIncreaseBackoff(1600);
  }

  @Test
  public void testFetchAckWithRetryBackoffRecovery() throws IOException {
    doCallRealMethod().when(receiver).fetchWithBackoff(any(), anyString());
    when(apiException.isRetryable()).thenReturn(true);

    doAnswer(new Answer<Void>() {
      int times = 0;

      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        times++;

        if (times != 4) {
          throw apiException;
        }

        return null;
      }
    }).when(receiver).fetchAndStoreMessages(any(), anyString());

    when(receiver.sleepAndIncreaseBackoff(anyInt()))
      .thenReturn(200)
      .thenReturn(400)
      .thenReturn(800)
      .thenReturn(200)
      .thenReturn(400)
      .thenReturn(800);

    //Stop after 3 iterations
    when(receiver.isStopped())
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(true);

    receiver.fetchWithBackoff(subscriber, "some-sub");

    verify(receiver, times(7)).fetchAndStoreMessages(eq(subscriber), eq("some-sub"));
    verify(backoffConfig, times(2)).getInitialBackoffMs();
    verify(receiver, times(6)).sleepAndIncreaseBackoff(anyInt());
    verify(receiver, times(2)).sleepAndIncreaseBackoff(100);
    verify(receiver, times(2)).sleepAndIncreaseBackoff(200);
    verify(receiver, times(2)).sleepAndIncreaseBackoff(400);
  }

  @Test(expected = ApiException.class)
  public void testFetchAckThrowsNonRetryableApiException() throws IOException {
    doCallRealMethod().when(receiver).fetchWithBackoff(any(), anyString());
    when(apiException.isRetryable()).thenReturn(false);

    when(statusCode.getCode()).thenReturn(StatusCode.Code.INVALID_ARGUMENT);
    ApiException apiException = new ApiException(new RuntimeException(""), statusCode, false);
    doThrow(apiException).when(receiver).fetchAndStoreMessages(any(), anyString());

    when(receiver.isStopped()).thenReturn(false);

    receiver.fetchWithBackoff(subscriber, "some-sub");
  }

  @Test
  public void testFetchAndAckWithAutoAcknowledge() throws IOException {
    doCallRealMethod().when(receiver).fetchAndStoreMessages(any(), anyString());
    when(receiver.isStopped()).thenReturn(false);

    //Set up messages list
    List<ReceivedMessage> messages = Arrays.asList(getReceivedMessage("a"), getReceivedMessage("b"));
    PullResponse response = PullResponse.newBuilder().addAllReceivedMessages(messages).buildPartial();

    when(pullCallable.call(any())).thenReturn(response);

    receiver.fetchAndStoreMessages(subscriber, "some-sub");

    verify(pullCallable, times(1)).call(pullRequestArgumentCaptor.capture());
    verify(receiver, times(1)).store(any(Iterator.class));
    verify(acknowledgeCallable, times(1)).call(acknowledgeRequestArgumentCaptor.capture());

    PullRequest pullRequest = pullRequestArgumentCaptor.getValue();
    Assert.assertEquals(pullRequest.getMaxMessages(), 12345);

    AcknowledgeRequest acknowledgeRequest = acknowledgeRequestArgumentCaptor.getValue();
    Assert.assertEquals(acknowledgeRequest.getAckIds(0), "a");
    Assert.assertEquals(acknowledgeRequest.getAckIds(1), "b");
  }

  @Test
  public void testFetchAndAckWithoutAutoAcknowledge() throws IOException {
    receiver = new PubSubReceiver("my-project", "my-topic", "my-subscription", credentials,
                                  false, level, backoffConfig, executor, subscriber, tokenBucket);

    doCallRealMethod().when(receiver).fetchAndStoreMessages(any(), anyString());
    when(receiver.isStopped()).thenReturn(false);

    //Set up messages list
    List<ReceivedMessage> messages = Arrays.asList(getReceivedMessage("a"), getReceivedMessage("b"));
    PullResponse response = PullResponse.newBuilder().addAllReceivedMessages(messages).buildPartial();

    when(pullCallable.call(any())).thenReturn(response);

    receiver.fetchAndStoreMessages(subscriber, "some-sub");

    verify(pullCallable, times(1)).call(pullRequestArgumentCaptor.capture());
    verify(receiver, times(1)).store(any(Iterator.class));
    verify(acknowledgeCallable, times(0)).call(acknowledgeRequestArgumentCaptor.capture());

    PullRequest pullRequest = pullRequestArgumentCaptor.getValue();
    Assert.assertEquals(pullRequest.getMaxMessages(), 12345);
  }

  @Test
  public void testFetchAndAckReturnsNoNewMessages() throws IOException {
    receiver = new PubSubReceiver("my-project", "my-topic", "my-subscription", credentials,
                                  false, level, backoffConfig, executor, subscriber, tokenBucket);

    doCallRealMethod().when(receiver).fetchAndStoreMessages(any(), anyString());
    when(receiver.isStopped()).thenReturn(false);

    //Set up messages list
    List<ReceivedMessage> messages = Collections.emptyList();
    PullResponse response = PullResponse.newBuilder().addAllReceivedMessages(messages).buildPartial();

    when(pullCallable.call(any())).thenReturn(response);

    receiver.fetchAndStoreMessages(subscriber, "some-sub");

    verify(pullCallable, times(1)).call(pullRequestArgumentCaptor.capture());
    verify(receiver, times(0)).store(any(Iterator.class));
    verify(acknowledgeCallable, times(0)).call(acknowledgeRequestArgumentCaptor.capture());
  }

  @Test
  public void testMessageRateCalculation() {
    doCallRealMethod().when(receiver).calculateUpdatedBackoff(anyInt());
    int backoff;

    when(backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(1000);

    backoff = 100;
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 200);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 400);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 800);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 1000);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 1000);

    when(backoffConfig.getInitialBackoffMs()).thenReturn(50);
    when(backoffConfig.getBackoffFactor()).thenReturn(4.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(950);

    backoff = 50;
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 200);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 800);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 950);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 950);
  }

}
