package software.amazon.glue.schema;

import java.time.Duration;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.DeleteSchemaResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaStatus;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private DeleteHandler handler;

    @Mock
    private GlueClient glueClient;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        glueClient = mock(GlueClient.class);
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new DeleteHandler();
    }

    @Test
    public void handleRequest_WhenDeleteSchemaSucceedsAndStabilizationSucceedsAfterNAttempts_ReturnsSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_SCHEMA_REQUEST,
            glueClient::deleteSchema)
        ).thenReturn(TestData.DELETE_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST,
            glueClient::getSchema)
        ).thenReturn(TestData.GET_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST,
            glueClient::getSchema)
        ).thenReturn(TestData.GET_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST,
            glueClient::getSchema)
        ).thenThrow(EntityNotFoundException.class);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(null);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenDeleteSchemaSucceedsAndStabilizationFails_ThrowsException() {
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_SCHEMA_REQUEST,
            glueClient::deleteSchema)
        ).thenReturn(TestData.DELETE_SCHEMA_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_REQUEST,
            glueClient::getSchema)
        ).thenThrow(AccessDeniedException.class);

        Exception exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler
                    .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient,
                        logger));
        assertThat(exception.getMessage())
            .contains(
                "Error occurred during operation 'AWS::Glue::Schema [arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema] deletion status couldn't be retrieved");
    }

    @Test
    public void handleRequest_WhenDeleteSchemaFails_ThrowsException() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DELETE_SCHEMA_REQUEST,
            glueClient::deleteSchema))
            .thenThrow(
                InvalidInputException
                    .builder()
                    .message("Invalid Schema")
                    .build());

        Exception exception = assertThrows(
            CfnInvalidRequestException.class,
            () -> handler
                .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception.getMessage())
            .contains("Invalid Schema");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String SCHEMA_NAME = "unit-test-schema";
        public final static String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        public final static DeleteSchemaRequest DELETE_SCHEMA_REQUEST =
            DeleteSchemaRequest.builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public final static DeleteSchemaResponse DELETE_SCHEMA_RESPONSE =
            DeleteSchemaResponse.builder()
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .status(SchemaStatus.DELETING)
                .build();

        public final static GetSchemaRequest GET_SCHEMA_REQUEST =
            GetSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                ).build();

        public final static ResourceModel RESOURCE_MODEL =
            ResourceModel.builder().arn(TestData.SCHEMA_ARN).build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        public static final GetSchemaResponse GET_SCHEMA_RESPONSE =
            GetSchemaResponse
                .builder()
                .registryName(REGISTRY_NAME)
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .build();
    }
}
