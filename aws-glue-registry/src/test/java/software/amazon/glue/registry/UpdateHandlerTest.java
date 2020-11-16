package software.amazon.glue.registry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetRegistryRequest;
import software.amazon.awssdk.services.glue.model.GetRegistryResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.RegistryStatus;
import software.amazon.awssdk.services.glue.model.UpdateRegistryRequest;
import software.amazon.awssdk.services.glue.model.UpdateRegistryResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.Map;

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
    public void handleRequest_ReturnsResponse_WhenInvoked() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_REGISTRY_REQUEST,
            glueClient::updateRegistry)
        ).thenReturn(TestData.UPDATE_REGISTRY_RESPONSE);

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.GET_REGISTRY_REQUEST,
            glueClient::getRegistry)
        ).thenReturn(TestData.GET_REGISTRY_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        final ResourceModel expectedResourceModel = ResourceModel
            .builder()
            .name(TestData.REGISTRY_NAME)
            .arn(TestData.REGISTRY_ARN)
            .description(TestData.NEW_REGISTRY_DESC)
            .build();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(expectedResourceModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ThrowsException_WhenUpdateRegistryFails() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.UPDATE_REGISTRY_REQUEST,
            glueClient::updateRegistry)
        ).thenThrow(
            InvalidInputException
                .builder()
                .message("Invalid description")
                .build()
        );

        Exception exception = assertThrows(
            CfnInvalidRequestException.class,
            () -> handler
                .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger)
        );

        assertThat(exception.getMessage())
            .contains("Invalid description");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String NEW_REGISTRY_DESC = "Unit testing registry updated.";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";
        public static final String CREATED_TIME = DateTime.now().toString();
        public static final String UPDATED_TIME = DateTime.now().toString();

        public final static ResourceModel INPUT_RESOURCE_MODEL = ResourceModel
            .builder()
            .name(REGISTRY_NAME)
            .arn(REGISTRY_ARN)
            .description(NEW_REGISTRY_DESC)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(INPUT_RESOURCE_MODEL)
                .build();

        public final static UpdateRegistryRequest UPDATE_REGISTRY_REQUEST =
            UpdateRegistryRequest
                .builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryName(TestData.REGISTRY_NAME)
                        .build()
                )
                .description(TestData.NEW_REGISTRY_DESC)
                .build();

        private final static UpdateRegistryResponse UPDATE_REGISTRY_RESPONSE =
            UpdateRegistryResponse
                .builder()
                .registryName(TestData.REGISTRY_NAME)
                .registryArn(TestData.REGISTRY_ARN)
                .build();

        public final static GetRegistryResponse GET_REGISTRY_RESPONSE =
            GetRegistryResponse
                .builder()
                .registryName(TestData.REGISTRY_NAME)
                .description(TestData.NEW_REGISTRY_DESC)
                .registryArn(REGISTRY_ARN)
                .status(RegistryStatus.AVAILABLE)
                .createdTime(TestData.CREATED_TIME)
                .updatedTime(TestData.UPDATED_TIME)
                .build();

        public final static GetRegistryRequest GET_REGISTRY_REQUEST =
            GetRegistryRequest
                .builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryName(REGISTRY_NAME)
                        .build())
                .build();
    }
}
