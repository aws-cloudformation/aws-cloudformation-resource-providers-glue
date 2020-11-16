package software.amazon.glue.schemaversion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.SchemaVersionStatus;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private ReadHandler handler;

    @Mock
    private GlueClient glueClient;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new ReadHandler();
    }

    @Test
    public void handleRequest_WhenVersionIdIsPresent_ReturnsResults() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion)
        ).thenReturn(TestData.getSchemaVersionResponseWithStatus(AVAILABLE));

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.RESPONSE_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenServiceCallFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion)
        ).thenThrow(AccessDeniedException.class);

        final Exception exception =
            assertThrows(
                CfnAccessDeniedException.class,
                () -> handler.handleRequest(
                    proxy,
                    TestData.RESOURCE_HANDLER_REQUEST,
                    new CallbackContext(), proxyClient, logger
                )
            );
        assertThat(exception.getMessage())
            .contains("Access denied for operation 'AWS::Glue::SchemaVersion'");
    }

    @Test
    public void handleRequest_WhenSchemaIsNotDefined_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST, glueClient::getSchemaVersion)
        ).thenThrow(InvalidInputException.class);

        assertThrows(
            CfnInvalidRequestException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST,
                new CallbackContext(), proxyClient, logger
            )
        );
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        public final static String SCHEMA_VERSION_ID = "307ce1bc-dc50-11ea-87d0-0242ac130003";
        public static final String SCHEMA_DEFINITION = "{\"type\": \"fixed\", \"size\": 16, \"name\": \"md5\"}";

        public static final ResourceModel RESOURCE_MODEL =
            ResourceModel
                .builder()
                .versionId(SCHEMA_VERSION_ID)
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        public static final ResourceModel RESPONSE_RESOURCE_MODEL =
            ResourceModel
                .builder()
                .versionId(SCHEMA_VERSION_ID)
                .schema(
                    Schema
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();

        public static final GetSchemaVersionRequest GET_SCHEMA_VERSION_REQUEST =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .build();

        public static final GetSchemaVersionResponse getSchemaVersionResponseWithStatus(SchemaVersionStatus status) {
            return GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .schemaArn(SCHEMA_ARN)
                .dataFormat(DataFormat.AVRO)
                .status(status)
                .createdTime(Instant.now().toString())
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();
        }
    }
}
