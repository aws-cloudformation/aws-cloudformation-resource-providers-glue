package software.amazon.glue.schemaversionmetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    @Mock
    private GlueClient glueClient;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new CreateHandler();
    }

    @Test
    public void handleRequest_WhenMetadataCreationSucceeds_ReturnSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST,
            glueClient::putSchemaVersionMetadata)
        ).thenReturn(TestData.PUT_SCHEMA_VERSION_METADATA_RESPONSE);

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
    public void handleRequest_WhenRequestFailsWithResourceLimitException_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST, glueClient::putSchemaVersionMetadata))
            .thenThrow(ResourceNumberLimitExceededException.class);

        Exception exception = assertThrows(
            CfnServiceLimitExceededException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Limit exceeded for resource of type 'AWS::Glue::SchemaVersionMetadata'.");
    }

    @Test
    public void handleRequest_WhenRequestFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST, glueClient::putSchemaVersionMetadata))
            .thenThrow(InternalServiceException.class);

        Exception exception = assertThrows(
            CfnGeneralServiceException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Error occurred during operation ");
    }

    @Test
    public void handleRequest_WhenRequestIsInvalid_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST, glueClient::putSchemaVersionMetadata))
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

    @Test
    public void handleRequest_WhenRequestFailsWithAlreadyExists_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST, glueClient::putSchemaVersionMetadata))
            .thenThrow(AlreadyExistsException.class);

        Exception exception = assertThrows(
            CfnAlreadyExistsException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Resource of type 'AWS::Glue::SchemaVersionMetadata' with identifier "
                + "'yurt9301-dc50-11ea-87d0-8iofb18nkrp8|META_KEY|META_VALUE' "
                + "already exists.");
    }

    @Test
    public void handleRequest_WhenRequestFailsWithNotFound_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST, glueClient::putSchemaVersionMetadata))
            .thenThrow(EntityNotFoundException.class);

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
            .contains("Resource of type 'AWS::Glue::SchemaVersionMetadata' with identifier "
                + "'yurt9301-dc50-11ea-87d0-8iofb18nkrp8|META_KEY|META_VALUE' "
                + "was not found.");
    }

    @Test
    public void handleRequest_WhenRequestFailsWithAccessDenied_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.PUT_SCHEMA_VERSION_METADATA_REQUEST, glueClient::putSchemaVersionMetadata))
            .thenThrow(AccessDeniedException.class);

        Exception exception = assertThrows(
            CfnAccessDeniedException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Access denied for operation 'AWS::Glue::SchemaVersionMetadata'");
    }

    private static class TestData {
        public final static String SCHEMA_VERSION_ID = "yurt9301-dc50-11ea-87d0-8iofb18nkrp8";
        private static final String METADATA_VALUE = "META_VALUE";
        private static final String METADATA_KEY = "META_KEY";
        public static final PutSchemaVersionMetadataRequest PUT_SCHEMA_VERSION_METADATA_REQUEST =
            PutSchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .metadataKeyValue(
                    MetadataKeyValuePair.builder()
                        .metadataValue(METADATA_VALUE)
                        .metadataKey(METADATA_KEY)
                        .build()
                )
                .build();

        public static final PutSchemaVersionMetadataResponse PUT_SCHEMA_VERSION_METADATA_RESPONSE =
            PutSchemaVersionMetadataResponse
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

