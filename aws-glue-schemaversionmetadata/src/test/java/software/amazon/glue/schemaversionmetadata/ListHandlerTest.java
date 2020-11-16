package software.amazon.glue.schemaversionmetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    @Mock
    private GlueClient glueClient;

    @Mock
    private Logger logger;

    private ListHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new ListHandler();
    }

    @Test
    public void handleRequest_WhenInvoked_ReturnsResults() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenReturn(TestData.QUERY_SCHEMA_VERSION_METADATA_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).containsOnlyElementsOf(TestData.RESOURCE_MODEL_LIST);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenMetadataListIsNull_ReturnsResults() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenReturn(TestData.NULL_METADATA_INFO_QUERY_SCHEMA_VERSION_METADATA_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).containsOnlyElementsOf(Collections.emptyList());
        assertThat(response.getNextToken()).isEqualTo(null);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenFailed_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenThrow(InternalServiceException.class);

        Exception exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger)
            );

        //Assert the correct arguments being passed.
        assertThat(exception.getMessage())
            .contains("Error occurred during operation ");
    }

    private static class TestData {
        public final static String SCHEMA_VERSION_ID = "yurt9301-dc50-11ea-87d0-8iofb18nkrp8";
        private static final String METADATA_KEY_1 = "META_KEY_1";
        private static final String METADATA_KEY_2 = "META_KEY_2";

        private static final String METADATA_VALUE_1 = "META_VALUE_1";
        private static final String METADATA_VALUE_2 = "META_VALUE_2";

        public static final String NEXT_TOKEN = "091230123921=";
        public static final String ANOTHER_NEXT_TOKEN = "sdf434g";

        private static final Integer MAX_RESULTS = 50;

        public static final QuerySchemaVersionMetadataRequest QUERY_SCHEMA_VERSION_METADATA_REQUEST =
            QuerySchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .nextToken(NEXT_TOKEN)
                .maxResults(MAX_RESULTS)
                .build();

        public static final QuerySchemaVersionMetadataResponse QUERY_SCHEMA_VERSION_METADATA_RESPONSE =
            QuerySchemaVersionMetadataResponse
                .builder()
                .nextToken(ANOTHER_NEXT_TOKEN)
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataInfoMap(
                    ImmutableMap.of(
                        METADATA_KEY_1,
                        MetadataInfo
                            .builder()
                            .metadataValue(METADATA_VALUE_1)
                            .build(),
                        METADATA_KEY_2,
                        MetadataInfo
                            .builder()
                            .metadataValue(METADATA_VALUE_2)
                            .build()
                    )
                )
                .build();

        private static final ResourceModel RESOURCE_MODEL =
            ResourceModel
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .key(METADATA_KEY_1)
                .value(METADATA_VALUE_1)
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .nextToken(NEXT_TOKEN)
                .build();

        public static final QuerySchemaVersionMetadataResponse
            NULL_METADATA_INFO_QUERY_SCHEMA_VERSION_METADATA_RESPONSE =
            QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .build();

        public static final List<ResourceModel> RESOURCE_MODEL_LIST =
            ImmutableList.of(
                ResourceModel
                    .builder()
                    .key(METADATA_KEY_2)
                    .value(METADATA_VALUE_2)
                    .schemaVersionId(SCHEMA_VERSION_ID)
                    .build(),

                ResourceModel
                    .builder()
                    .key(METADATA_KEY_1)
                    .value(METADATA_VALUE_1)
                    .schemaVersionId(SCHEMA_VERSION_ID)
                    .build()
            );
    }

}
