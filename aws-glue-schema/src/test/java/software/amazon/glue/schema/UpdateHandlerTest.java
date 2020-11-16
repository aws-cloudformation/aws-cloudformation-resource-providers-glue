package software.amazon.glue.schema;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaStatus;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;
import software.amazon.awssdk.services.glue.model.UpdateSchemaRequest;
import software.amazon.awssdk.services.glue.model.UpdateSchemaResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
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

import software.amazon.glue.schema.ResourceModel;
import software.amazon.glue.schema.SchemaVersion;
import software.amazon.glue.schema.Registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private UpdateHandler handler;

    @Mock
    private GlueClient glueClient;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        glueClient = mock(GlueClient.class);
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new UpdateHandler();
    }

    @Test
    public void handleRequest_whenCompatibilityIsUpdatedToLatestVersion_ReturnsResponse() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_SCHEMA_REQUEST_WITH_COMPATIBILITY_LATEST_VERSION,
            glueClient::updateSchema)
        ).thenReturn(TestData.UPDATE_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST_WITH_ARN,
            glueClient::getSchema)
        ).thenReturn(TestData.GET_SCHEMA_RESPONSE);

        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_TO_UPDATE_COMPATIBILITY_WITH_LATEST_VERSION,
            new CallbackContext(),
            proxyClient,
            logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_whenVersionNumberIsUpdated_ReturnsResponse() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_SCHEMA_REQUEST_WITH_VERSION_NUMBER,
            glueClient::updateSchema)
        ).thenReturn(TestData.UPDATE_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST_WITH_ARN,
            glueClient::getSchema)
        ).thenReturn(TestData.GET_SCHEMA_RESPONSE);

        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_TO_UPDATE_VERSION_NUMBER,
            new CallbackContext(),
            proxyClient,
            logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_whenDescriptionIsUpdated_ReturnsSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_SCHEMA_REQUEST_TO_UPDATE_DESCRIPTION,
            glueClient::updateSchema)
        ).thenReturn(TestData.UPDATE_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST_WITH_ARN,
            glueClient::getSchema)
        ).thenReturn(TestData.GET_SCHEMA_RESPONSE_WITH_DESC_UPDATED);

        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_TO_UPDATE_DESCRIPTION,
            new CallbackContext(),
            proxyClient,
            logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_RESOURCE_MODEL_WITH_DESC_UPDATED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_whenRegistryNameIsProvided_InvokesUpdateSchema() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_SCHEMA_REQUEST_WITH_REGISTRY,
            glueClient::updateSchema)
        ).thenReturn(TestData.UPDATE_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST_WITH_REGISTRY,
            glueClient::getSchema)
        ).thenReturn(TestData.GET_SCHEMA_RESPONSE);

        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITH_REGISTRY,
            new CallbackContext(),
            proxyClient,
            logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_whenUpdateSchemaFails_ThrowsException() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_SCHEMA_REQUEST_WITH_NO_REGISTRY,
            glueClient::updateSchema)
        ).thenThrow(
            InvalidInputException
                .builder()
                .message(TestData.ERROR_MSG)
                .build()
        );

        Exception exception = assertThrows(CfnInvalidRequestException.class,
            () ->
                handler.handleRequest(
                    proxy,
                    TestData.RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY,
                    new CallbackContext(),
                    proxyClient,
                    logger
                )
        );

        assertThat(exception.getMessage())
            .contains(TestData.ERROR_MSG);
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String DEFAULT_REGISTRY_NAME = "default-registry";
        public final static String DEFAULT_REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/default-registry";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";
        public final static String SCHEMA_NAME = "unit-test-schema";
        public final static String SCHEMA_DESC = "Creating a unit-test schema";
        public static final String ERROR_MSG = "Invalid schema description";
        private static final String DATA_FORMAT = "Avro";
        private static final String COMPATIBILITY = "BACKWARD_ALL";
        private static final Long LATEST_VERSION = 1l;
        private static final Long NEXT_VERSION = 2l;
        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        private static final Long CHECKPOINT_VERSION = 1l;
        private static final String NEW_SCHEMA_DESC = "Updated schema description";

        private static final Integer VERSION_NUMBER_TO_UPDATE = 9;

        private static final String CREATED_TIME = "123409018203";
        private static final String UPDATED_TIME = "128884444433";
        private static final String SCHEMA_VERSION_ID = "123e4567-e89b-12d3-a456-426614174000";

        public final static ResourceModel RESOURCE_MODEL_WITH_NO_REGISTRY =
            ResourceModel
                .builder()
                .name(SCHEMA_NAME)
                .arn(SCHEMA_ARN)
                .description(SCHEMA_DESC)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .isLatest(true)
                        .build()
                )
                .compatibility(COMPATIBILITY)
                .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_NO_REGISTRY)
                .build();

        public final static UpdateSchemaRequest UPDATE_SCHEMA_REQUEST_WITH_NO_REGISTRY =
            UpdateSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .schemaVersionNumber(
                    SchemaVersionNumber
                        .builder()
                        .latestVersion(true)
                        .build()
                )
                .description(SCHEMA_DESC)
                .compatibility(COMPATIBILITY)
                .build();

        public static final GetSchemaRequest GET_SCHEMA_REQUEST_WITH_REGISTRY =
            GetSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public final static ResourceModel RESOURCE_MODEL_WITH_REGISTRY =
            ResourceModel
                .builder()
                .name(SCHEMA_NAME)
                .arn(SCHEMA_ARN)
                .description(SCHEMA_DESC)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .isLatest(true)
                        .build()
                )
                .registry(Registry.builder().name(REGISTRY_NAME).build())
                .compatibility(COMPATIBILITY)
                .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_REGISTRY =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_REGISTRY)
                .build();

        public final static UpdateSchemaRequest UPDATE_SCHEMA_REQUEST_WITH_REGISTRY =
            UpdateSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .schemaVersionNumber(
                    SchemaVersionNumber
                        .builder()
                        .latestVersion(true)
                        .build()
                )
                .description(SCHEMA_DESC)
                .compatibility(COMPATIBILITY)
                .build();

        public static final UpdateSchemaResponse UPDATE_SCHEMA_RESPONSE =
            UpdateSchemaResponse
                .builder()
                .registryName(REGISTRY_NAME)
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .build();

        public final static ResourceModel
            RESOURCE_MODEL_TO_UPDATE_COMPATIBILITY_WITH_LATEST_VERSION =
            ResourceModel
                .builder()
                .arn(SCHEMA_ARN)
                .checkpointVersion(
                    software.amazon.glue.schema.SchemaVersion
                        .builder()
                        .isLatest(true)
                        .build()
                )
                .compatibility(Compatibility.FORWARD.toString())
                .build();

        public final static ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_REQUEST_TO_UPDATE_COMPATIBILITY_WITH_LATEST_VERSION =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_TO_UPDATE_COMPATIBILITY_WITH_LATEST_VERSION)
                .build();

        public static final UpdateSchemaRequest
            UPDATE_SCHEMA_REQUEST_WITH_COMPATIBILITY_LATEST_VERSION =
            UpdateSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .compatibility(Compatibility.FORWARD)
                .schemaVersionNumber(
                    SchemaVersionNumber
                        .builder()
                        .latestVersion(true)
                        .build()
                )
                .build();

        public final static GetSchemaVersionRequest GET_SCHEMA_VERSION_REQUEST =
            GetSchemaVersionRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .schemaVersionNumber(
                    SchemaVersionNumber
                        .builder()
                        .versionNumber(1L)
                        .build()
                )
                .build();

        public final static GetSchemaVersionResponse GET_SCHEMA_VERSION_RESPONSE =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .schemaArn(SCHEMA_ARN)
                .build();

        public final static ResourceModel
            RESOURCE_MODEL_TO_UPDATE_VERSION_NUMBER =
            ResourceModel
                .builder()
                .arn(SCHEMA_ARN)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .versionNumber(VERSION_NUMBER_TO_UPDATE)
                        .build()
                )
                .compatibility(Compatibility.FORWARD.toString())
                .build();

        public final static ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_REQUEST_TO_UPDATE_VERSION_NUMBER =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_TO_UPDATE_VERSION_NUMBER)
                .build();

        public static final UpdateSchemaRequest
            UPDATE_SCHEMA_REQUEST_WITH_VERSION_NUMBER =
            UpdateSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .compatibility(Compatibility.FORWARD)
                .schemaVersionNumber(
                    SchemaVersionNumber
                        .builder()
                        .versionNumber(Long.valueOf(VERSION_NUMBER_TO_UPDATE))
                        .build()
                )
                .build();

        public final static ResourceModel
            RESOURCE_MODEL_TO_UPDATE_DESCRIPTION =
            ResourceModel
                .builder()
                .arn(SCHEMA_ARN)
                .description(NEW_SCHEMA_DESC)
                .build();

        public final static ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_REQUEST_TO_UPDATE_DESCRIPTION =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_TO_UPDATE_DESCRIPTION)
                .build();

        public static final UpdateSchemaRequest
            UPDATE_SCHEMA_REQUEST_TO_UPDATE_DESCRIPTION =
            UpdateSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .description(NEW_SCHEMA_DESC)
                .build();

        public static final GetSchemaResponse GET_SCHEMA_RESPONSE_WITH_DESC_UPDATED =
            GetSchemaResponse
                .builder()
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .description(NEW_SCHEMA_DESC)
                .registryArn(REGISTRY_ARN)
                .registryName(REGISTRY_NAME)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .latestSchemaVersion(LATEST_VERSION)
                .nextSchemaVersion(NEXT_VERSION)
                .dataFormat(DATA_FORMAT)
                .compatibility(COMPATIBILITY)
                .schemaCheckpoint(CHECKPOINT_VERSION)
                .createdTime(CREATED_TIME)
                .updatedTime(UPDATED_TIME)
                .build();

        public static final ResourceModel GET_SCHEMA_RESOURCE_MODEL_WITH_DESC_UPDATED =
            ResourceModel
                .builder()
                .name(SCHEMA_NAME)
                .arn(SCHEMA_ARN)
                .compatibility(COMPATIBILITY)
                .registry(
                    Registry
                        .builder()
                        .arn(REGISTRY_ARN)
                        .build()
                )
                .description(NEW_SCHEMA_DESC)
                .dataFormat(DATA_FORMAT)
                .initialSchemaVersionId(SCHEMA_VERSION_ID)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .versionNumber(LATEST_VERSION.intValue())
                        .isLatest(true)
                        .build()
                )
                .build();

        public static final GetSchemaRequest GET_SCHEMA_REQUEST_WITH_ARN =
            GetSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public static final GetSchemaResponse GET_SCHEMA_RESPONSE =
            GetSchemaResponse
                .builder()
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .registryArn(REGISTRY_ARN)
                .registryName(REGISTRY_NAME)
                .description(SCHEMA_DESC)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .latestSchemaVersion(LATEST_VERSION)
                .nextSchemaVersion(NEXT_VERSION)
                .dataFormat(DATA_FORMAT)
                .compatibility(COMPATIBILITY)
                .schemaCheckpoint(CHECKPOINT_VERSION)
                .createdTime(CREATED_TIME)
                .updatedTime(UPDATED_TIME)
                .build();

        public static final ResourceModel GET_SCHEMA_RESOURCE_MODEL =
            ResourceModel
                .builder()
                .name(SCHEMA_NAME)
                .arn(SCHEMA_ARN)
                .compatibility(COMPATIBILITY)
                .registry(
                    Registry
                        .builder()
                        .arn(REGISTRY_ARN)
                        .build()
                )
                .description(SCHEMA_DESC)
                .dataFormat(DATA_FORMAT)
                .initialSchemaVersionId(SCHEMA_VERSION_ID)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .versionNumber(LATEST_VERSION.intValue())
                        .isLatest(true)
                        .build()
                )
                .build();
    }
}
