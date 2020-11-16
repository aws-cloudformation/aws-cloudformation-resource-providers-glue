package software.amazon.glue.schemaversionmetadata;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
    private ClientBuilder() {
    }

    //It is recommended to use static HTTP client so less memory is consumed.
    public static GlueClient getClient() {
        return
            GlueClient
                .builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .build();
    }
}
