package software.amazon.glue.schema;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaStatus;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.glue.schema.ResourceModel;
import software.amazon.glue.schema.Registry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

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
    public void handleRequest_WhenValidSchemaArnIsProvided_ReturnsSchema() {

        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_REQUEST_WITH_ARN, glueClient::getSchema))
            .thenReturn(TestData.GET_SCHEMA_RESPONSE);

        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_WITH_SCHEMA_ARN, new CallbackContext(), proxyClient,
                logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_RESPONSE_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenGetSchemaFails_ThrowsException() {

        when(proxyClient.injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_REQUEST_WITH_ARN, glueClient::getSchema))
            .thenThrow(EntityNotFoundException.class);

        final Exception exception =
            assertThrows(CfnNotFoundException.class, () ->
                handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_WITH_SCHEMA_ARN, new CallbackContext(),
                    proxyClient, logger));

        assertThat(exception.getMessage())
            .contains("Resource of type 'AWS::Glue::Schema' with identifier "
                + "'SchemaId(SchemaArn=arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema)' was not found.");
}

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";
        public final static String SCHEMA_NAME = "unit-test-schema";
        public final static String SCHEMA_DESC = "Creating a unit-test schema";

        private static final String DATA_FORMAT = "Avro";
        private static final String COMPATIBILITY = "BACKWARD_ALL";
        private static final Long LATEST_VERSION = 1l;
        private static final Long NEXT_VERSION = 2l;
        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        private static final Long CHECKPOINT_VERSION = 1l;
        private static final String SCHEMA_VERSION_ID = "123e4567-e89b-12d3-a456-426614174000";

        public final static ResourceModel RESOURCE_MODEL_WITH_SCHEMA_NAME = ResourceModel
            .builder()
            .name(SCHEMA_NAME)
            .registry(
                Registry
                    .builder()
                    .name(REGISTRY_NAME)
                    .build()
            )
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_WITH_SCHEMA_NAME =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_SCHEMA_NAME)
                .build();

        public final static GetSchemaRequest GET_SCHEMA_REQUEST_WITH_NAME =
            GetSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public final static ResourceModel RESOURCE_MODEL_WITH_SCHEMA_ARN = ResourceModel
            .builder()
            .arn(SCHEMA_ARN)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_WITH_SCHEMA_ARN =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_SCHEMA_ARN)
                .build();

        public final static GetSchemaRequest GET_SCHEMA_REQUEST_WITH_ARN =
            GetSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public final static GetSchemaResponse GET_SCHEMA_RESPONSE =
            GetSchemaResponse.builder()
                .compatibility(COMPATIBILITY)
                .description(SCHEMA_DESC)
                .latestSchemaVersion(LATEST_VERSION)
                .nextSchemaVersion(NEXT_VERSION)
                .registryArn(REGISTRY_ARN)
                .registryName(REGISTRY_NAME)
                .schemaArn(SCHEMA_ARN)
                .schemaCheckpoint(CHECKPOINT_VERSION)
                .schemaName(SCHEMA_NAME)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .dataFormat(DATA_FORMAT)
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

        public final static ResourceModel GET_SCHEMA_RESPONSE_RESOURCE_MODEL =
            ResourceModel
                .builder()
                .name(TestData.SCHEMA_NAME)
                .compatibility(TestData.COMPATIBILITY)
                .registry(
                    Registry
                        .builder()
                        .arn(TestData.REGISTRY_ARN)
                        .build()
                )
                .description(TestData.SCHEMA_DESC)
                .dataFormat(TestData.DATA_FORMAT)
                .arn(TestData.SCHEMA_ARN)
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
