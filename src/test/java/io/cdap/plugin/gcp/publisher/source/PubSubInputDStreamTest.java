package io.cdap.plugin.gcp.publisher.source;

import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.StreamingContextState;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class PubSubInputDStreamTest {

/*  @Test
  public void testInit() {
    StreamingContext ctx = Mockito.mock(StreamingContext.class);
    when(ctx.getState()).thenReturn(StreamingContextState.INITIALIZED);
    ServiceAccountCredentials creds = Mockito.mock(ServiceAccountCredentials.class);

    PubSubInputDStream instance =
      new PubSubInputDStream(ctx, "p", "t", "s", creds, StorageLevel.MEMORY_ONLY(), false);
    System.out.println(instance);
  }*/
}
