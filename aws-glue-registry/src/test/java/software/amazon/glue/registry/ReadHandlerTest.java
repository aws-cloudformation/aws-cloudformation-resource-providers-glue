package software.amazon.glue.registry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetRegistryRequest;
import software.amazon.awssdk.services.glue.model.GetRegistryResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.RegistryStatus;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private ReadHandler handler;

    @Mock
    private GlueClient sdkClient;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        sdkClient = mock(GlueClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
        handler = new ReadHandler();
    }

    @Test
    public void handleRequest_ReturnsRegistrySuccessfully_WhenResourceModelIsPassed() {

        Mockito.when(
            proxyClient.injectCredentialsAndInvokeV2(
                TestData.GET_REGISTRY_REQUEST, sdkClient::getRegistry))
            .thenReturn(TestData.GET_REGISTRY_RESPONSE);

        final ResourceModel expectedResponseModel = ResourceModel
            .builder()
            .name(TestData.REGISTRY_NAME)
            .arn(TestData.REGISTRY_ARN)
            .description(TestData.REGISTRY_DESC)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(expectedResponseModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ReturnsRegistrySuccessfully_WhenResourceModelWithoutTagsIsPassed() {

        Mockito.when(
            proxyClient.injectCredentialsAndInvokeV2(
                TestData.GET_REGISTRY_REQUEST, sdkClient::getRegistry))
            .thenReturn(TestData.GET_REGISTRY_RESPONSE_WITHOUT_TAGS);

        final ResourceModel expectedResponseModel = ResourceModel
            .builder()
            .name(TestData.REGISTRY_NAME)
            .arn(TestData.REGISTRY_ARN)
            .description(TestData.REGISTRY_DESC)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(expectedResponseModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ThrowsException_WhenGetRegistryFails() {
        Mockito.when(
            proxyClient.injectCredentialsAndInvokeV2(
                TestData.GET_REGISTRY_REQUEST, sdkClient::getRegistry))
            .thenThrow(EntityNotFoundException.class);

        Exception exception = assertThrows(CfnNotFoundException.class, () ->
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger)
        );

        assertThat(exception.getMessage())
            .contains("Resource of type 'AWS::Glue::Registry' with identifier 'unit-test-registry' was not found.");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String REGISTRY_DESC = "Unit testing registry creation.";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";

        public final static ResourceModel RESOURCE_MODEL = ResourceModel
            .builder()
            .name(REGISTRY_NAME)
            .arn(REGISTRY_ARN)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        public final static GetRegistryRequest GET_REGISTRY_REQUEST =
            GetRegistryRequest.builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryName(REGISTRY_NAME)
                        .build())
                .build();

        private final static GetRegistryResponse GET_REGISTRY_RESPONSE =
            GetRegistryResponse
                .builder()
                .registryName(TestData.REGISTRY_NAME)
                .description(TestData.REGISTRY_DESC)
                .registryArn(TestData.REGISTRY_ARN)
                .status(RegistryStatus.AVAILABLE)
                .build();

        private final static GetRegistryResponse GET_REGISTRY_RESPONSE_WITHOUT_TAGS =
            GetRegistryResponse
                .builder()
                .registryName(TestData.REGISTRY_NAME)
                .description(TestData.REGISTRY_DESC)
                .registryArn(TestData.REGISTRY_ARN)
                .status(RegistryStatus.AVAILABLE)
                .build();
    }
}
