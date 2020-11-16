package software.amazon.glue.registry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.DeleteRegistryRequest;
import software.amazon.awssdk.services.glue.model.DeleteRegistryResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetRegistryRequest;
import software.amazon.awssdk.services.glue.model.GetRegistryResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;

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
    public void handleRequest_WhenDeleteRegistrySucceedsAndStabilizationSucceedsAfterNAttempts_ReturnsSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_REGISTRY_REQUEST,
            glueClient::deleteRegistry)
        ).thenReturn(TestData.DELETE_REGISTRY_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_REGISTRY_REQUEST,
            glueClient::getRegistry)
        ).thenReturn(TestData.GET_REGISTRY_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_REGISTRY_REQUEST,
            glueClient::getRegistry)
        ).thenReturn(TestData.GET_REGISTRY_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_REGISTRY_REQUEST,
            glueClient::getRegistry)
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
    public void handleRequest_WhenDeleteRegistrySucceedsAndStabilizationFails_ThrowsException() {
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_REGISTRY_REQUEST,
            glueClient::deleteRegistry)
        ).thenReturn(TestData.DELETE_REGISTRY_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_REGISTRY_REQUEST,
            glueClient::getRegistry)
        ).thenThrow(AccessDeniedException.class);

        Exception exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler
                    .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient,
                        logger));
        assertThat(exception.getMessage())
            .contains(
                "Error occurred during operation 'AWS::Glue::Registry [unit-test-registry] deletion status couldn't be retrieved");
    }

    @Test
    public void handleRequest_WhenDeleteRegistryFails_ThrowsException() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DELETE_REGISTRY_REQUEST,
            glueClient::deleteRegistry))
            .thenThrow(
                InvalidInputException
                    .builder()
                    .message("Invalid Registry")
                    .build());

        Exception exception = assertThrows(
            CfnInvalidRequestException.class,
            () -> handler
                .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception.getMessage())
            .contains("Invalid Registry");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";
        public final static DeleteRegistryRequest DELETE_REGISTRY_REQUEST =
            DeleteRegistryRequest.builder()
                .registryId(
                    RegistryId.builder()
                        .registryName(REGISTRY_NAME)
                        .build()
                ).build();

        public final static DeleteRegistryResponse DELETE_REGISTRY_RESPONSE =
            DeleteRegistryResponse.builder()
                .registryName(REGISTRY_NAME)
                .registryArn(REGISTRY_ARN)
                .build();

        public final static GetRegistryRequest GET_REGISTRY_REQUEST =
            GetRegistryRequest.builder()
                .registryId(
                    RegistryId.builder()
                        .registryName(REGISTRY_NAME)
                        .build()
                ).build();

        public final static ResourceModel RESOURCE_MODEL = ResourceModel.builder().name(REGISTRY_NAME).build();
        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        public static final GetRegistryResponse GET_REGISTRY_RESPONSE =
            GetRegistryResponse
                .builder()
                .build();
    }
}
