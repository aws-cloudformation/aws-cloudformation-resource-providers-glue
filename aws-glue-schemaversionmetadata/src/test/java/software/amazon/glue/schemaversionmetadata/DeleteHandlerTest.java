package software.amazon.glue.schemaversionmetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.RemoveSchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.RemoveSchemaVersionMetadataResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    private GlueClient glueClient;

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new DeleteHandler();
    }

    @Test
    public void handleRequest_WhenMetdataIsDeleted_ReturnsSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.REMOVE_SCHEMA_VERSION_METADATA_REQUEST,
            glueClient::removeSchemaVersionMetadata)
        ).thenReturn(TestData.REMOVE_SCHEMA_VERSION_METADATA_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST,
            new CallbackContext(), proxyClient, logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(null);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenMetdataDeleteFails_ThrowsException() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.REMOVE_SCHEMA_VERSION_METADATA_REQUEST,
            glueClient::removeSchemaVersionMetadata)
        ).thenThrow(InternalServiceException.class);

        Exception exception = assertThrows(CfnGeneralServiceException.class,
            () -> handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST,
            new CallbackContext(), proxyClient, logger
        ));

        assertThat(exception.getMessage()).isEqualTo("Error occurred during operation 'null'.");

    }

    private static class TestData {
        public final static String SCHEMA_VERSION_ID = "yurt9301-dc50-11ea-87d0-8iofb18nkrp8";
        private static final String METADATA_VALUE = "META_VALUE";
        private static final String METADATA_KEY = "META_KEY";

        private static final MetadataKeyValuePair METADATA_KEY_VALUE_PAIR =
            MetadataKeyValuePair.builder()
                .metadataKey(METADATA_KEY)
                .metadataValue(METADATA_VALUE)
                .build();
        public static final RemoveSchemaVersionMetadataRequest REMOVE_SCHEMA_VERSION_METADATA_REQUEST =
            RemoveSchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataKeyValue(METADATA_KEY_VALUE_PAIR)
                .build();

        public static final RemoveSchemaVersionMetadataResponse REMOVE_SCHEMA_VERSION_METADATA_RESPONSE =
            RemoveSchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataKey(METADATA_KEY)
                .metadataValue(METADATA_VALUE)
                .build();

        private static final ResourceModel RESOURCE_MODEL =
            ResourceModel
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .key(METADATA_KEY)
                .value(METADATA_VALUE)
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();
    }
}
