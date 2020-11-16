package software.amazon.glue.schemaversionmetadata;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    private GlueClient glueClient;

    private ReadHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new ReadHandler();
    }

    @Test
    public void handleRequest_WhenMetadataIsPresent_ReturnsSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST,
            glueClient::querySchemaVersionMetadata)
        ).thenReturn(TestData.QUERY_SCHEMA_VERSION_METADATA_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST,
            new CallbackContext(), proxyClient, logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenMetadataIsNotFound_ThrowsNotFoundException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenReturn(TestData.EMPTY_QUERY_SCHEMA_VERSION_METADATA_RESPONSE);

        Exception exception = assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .isEqualTo("Resource of type 'AWS::Glue::SchemaVersionMetadata' with identifier "
                + "'yurt9301-dc50-11ea-87d0-8iofb18nkrp8|META_KEY|META_VALUE' was not found.");
    }

    @Test
    public void handleRequest_WhenMetadataIsChangedOutOfBand_ThrowsNotFoundException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenReturn(TestData.DIFFERENT_QUERY_SCHEMA_VERSION_METADATA_RESPONSE);

        Exception exception = assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .isEqualTo("Resource of type 'AWS::Glue::SchemaVersionMetadata' with identifier "
                + "'yurt9301-dc50-11ea-87d0-8iofb18nkrp8|META_KEY|META_VALUE' was not found.");
    }

    @Test
    public void handleRequest_WhenMetadataIsNull_ThrowsNotFoundException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenReturn(TestData.NULL_METADATA_INFO_QUERY_SCHEMA_VERSION_METADATA_RESPONSE);

        Exception exception = assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .isEqualTo("Resource of type 'AWS::Glue::SchemaVersionMetadata' with identifier "
                + "'yurt9301-dc50-11ea-87d0-8iofb18nkrp8|META_KEY|META_VALUE' was not found.");
    }

    @Test
    public void handleRequest_WhenRequestFailsWithInvalidInput_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.QUERY_SCHEMA_VERSION_METADATA_REQUEST, glueClient::querySchemaVersionMetadata))
            .thenThrow(InvalidInputException.class);

        assertThrows(
            CfnInvalidRequestException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );
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
        public static final QuerySchemaVersionMetadataRequest QUERY_SCHEMA_VERSION_METADATA_REQUEST =
            QuerySchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataList(METADATA_KEY_VALUE_PAIR)
                .build();

        public static final QuerySchemaVersionMetadataResponse QUERY_SCHEMA_VERSION_METADATA_RESPONSE =
            QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataInfoMap(
                    ImmutableMap.of(
                        METADATA_KEY,
                        MetadataInfo
                            .builder()
                            .metadataValue(METADATA_VALUE)
                            .build()
                    )
                )
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

        private static final QuerySchemaVersionMetadataResponse
            EMPTY_QUERY_SCHEMA_VERSION_METADATA_RESPONSE =
            QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataInfoMap(
                    ImmutableMap.of()
                )
                .build();

        public static final QuerySchemaVersionMetadataResponse
            NULL_METADATA_INFO_QUERY_SCHEMA_VERSION_METADATA_RESPONSE =
            QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .build();

        private static final QuerySchemaVersionMetadataResponse
            DIFFERENT_QUERY_SCHEMA_VERSION_METADATA_RESPONSE =
            QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataInfoMap(
                    ImmutableMap.of(
                        METADATA_KEY,
                        MetadataInfo
                            .builder()
                            .metadataValue("Some other value")
                            .build()
                    )
                )
                .build();
    }

}
