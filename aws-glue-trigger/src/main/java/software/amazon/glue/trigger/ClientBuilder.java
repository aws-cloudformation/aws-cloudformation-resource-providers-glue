package software.amazon.glue.trigger;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.cloudformation.AbstractWrapper;

import java.time.Duration;

public class ClientBuilder {
    private ClientBuilder() {
        throw new IllegalStateException("Utility class");
    }

    private static final BackoffStrategy GLUE_CLIENT_BACKOFF_THROTTLING_STRATEGY =
            EqualJitterBackoffStrategy.builder()
                    .baseDelay(Duration.ofMillis(2000)) // 1st retry is ~2 sec
                    .maxBackoffTime(SdkDefaultRetrySetting.MAX_BACKOFF) // default is 20s
                    .build();

    private static final RetryPolicy GLUE_CLIENT_RETRY_POLICY =
            RetryPolicy.builder()
                    .numRetries(4)
                    .retryCondition(RetryCondition.defaultRetryCondition())
                    .throttlingBackoffStrategy(GLUE_CLIENT_BACKOFF_THROTTLING_STRATEGY)
                    .build();

    public static GlueClient getClient() {
        return GlueClient.builder()
                .httpClient(AbstractWrapper.HTTP_CLIENT)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(GLUE_CLIENT_RETRY_POLICY)
                        .build())
                .build();
    }
}
