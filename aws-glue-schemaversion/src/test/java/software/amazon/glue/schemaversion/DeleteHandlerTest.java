package software.amazon.glue.schemaversion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.DeleteSchemaVersionsRequest;
import software.amazon.awssdk.services.glue.model.DeleteSchemaVersionsResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.SchemaVersionErrorItem;
import software.amazon.awssdk.services.glue.model.SchemaVersionStatus;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
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
    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private DeleteHandler handler;

    @Mock
    private GlueClient glueClient;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new DeleteHandler();
    }

    @Test
    public void handleRequest_WhenDeleteSchemaVersionSucceedsAndStabilizationSucceedsAfterNAttempts_ReturnsSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_SCHEMA_VERSION_REQUEST,
            glueClient::deleteSchemaVersions)
        ).thenReturn(TestData.DELETE_SCHEMA_VERSIONS_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST,
            glueClient::getSchemaVersion)
        )
            //First attempt
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE)
            //Second attempt
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE)
            //Third attempt
            .thenThrow(EntityNotFoundException.class);

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
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST,
            glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE)
            .thenThrow(AccessDeniedException.builder().message("Invalid Cred").build());

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_SCHEMA_VERSION_REQUEST,
            glueClient::deleteSchemaVersions)
        ).thenReturn(TestData.DELETE_SCHEMA_VERSIONS_RESPONSE);

        Exception exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler
                    .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient,
                        logger)
            );
        assertThat(exception.getMessage())
            .contains(
                "'AWS::Glue::SchemaVersion [6eff3f2b-89c0-40ea-a268-9eb34b9cdd2d] deletion status couldn't be retrieved: Invalid Cred'.");
    }

    @Test
    public void handleRequest_WhenDeleteSchemaFails_ThrowsException() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.GET_SCHEMA_VERSION_REQUEST,
            glueClient::getSchemaVersion))
            .thenReturn(TestData.GET_SCHEMA_VERSION_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DELETE_SCHEMA_VERSION_REQUEST,
            glueClient::deleteSchemaVersions))
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
        public final static String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        private static final Long VERSION_NUMBER = 2L;

        public final static DeleteSchemaVersionsRequest DELETE_SCHEMA_VERSION_REQUEST =
            DeleteSchemaVersionsRequest
                .builder()
                .schemaId(software.amazon.awssdk.services.glue.model.SchemaId
                    .builder()
                    .schemaArn(SCHEMA_ARN)
                    .build()
                )
                .versions(VERSION_NUMBER.toString())
                .build();

        public final static DeleteSchemaVersionsResponse DELETE_SCHEMA_VERSIONS_RESPONSE =
            DeleteSchemaVersionsResponse.builder()
                .schemaVersionErrors(
                    SchemaVersionErrorItem
                        .builder()
                        .versionNumber(VERSION_NUMBER)
                        .errorDetails(ErrorDetails
                            .builder().build())
                        .build()
                )
                .build();

        private static final String VERSION_ID = "6eff3f2b-89c0-40ea-a268-9eb34b9cdd2d";

        public final static GetSchemaVersionRequest GET_SCHEMA_VERSION_REQUEST =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(VERSION_ID)
                .build();

        public final static ResourceModel RESOURCE_MODEL =
            ResourceModel
                .builder()
                .versionId(TestData.VERSION_ID)
                .schema(
                    Schema
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        public static final GetSchemaVersionResponse GET_SCHEMA_VERSION_RESPONSE =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(VERSION_ID)
                .status(SchemaVersionStatus.DELETING)
                .schemaArn(SCHEMA_ARN)
                .versionNumber(VERSION_NUMBER)
                .build();
    }
}
