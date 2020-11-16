package software.amazon.glue.registry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.ListRegistriesRequest;
import software.amazon.awssdk.services.glue.model.ListRegistriesResponse;
import software.amazon.awssdk.services.glue.model.RegistryListItem;
import software.amazon.awssdk.services.glue.model.RegistryStatus;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private ListHandler handler;

    @Mock
    private GlueClient glueClient;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        handler = new ListHandler();
        proxyClient = MOCK_PROXY(proxy, glueClient);
    }

    @Test
    public void handleRequest_ReturnsResultsSuccessfully_WhenInvoked() {
        ListRegistriesResponse listRegistriesResponse = ListRegistriesResponse
            .builder()
            .nextToken(TestData.ANOTHER_NEXT_TOKEN)
            .registries(TestData.REGISTRIES)
            .build();

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_REGISTRIES_REQUEST, glueClient::listRegistries
        )).thenReturn(listRegistriesResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        //Assert the response.
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.REGISTRIES_MODELS);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ThrowsException_WhenListRegistriesFails() {
        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.LIST_REGISTRIES_REQUEST, glueClient::listRegistries))
            .thenThrow(InternalServiceException.class);

        Exception exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger)
            );

        assertThat(exception.getMessage())
            .contains("Error occurred during operation");
    }

    private static class TestData {
        public static final String NEXT_TOKEN = "1231j091j23";
        public static final String ANOTHER_NEXT_TOKEN = "09018023nj";
        public static final ListRegistriesRequest LIST_REGISTRIES_REQUEST = ListRegistriesRequest
            .builder()
            .nextToken(NEXT_TOKEN)
            .maxResults(50)
            .build();
        private static final String REGISTRY_NAME = "Unit-testing-registry";
        private static final String REGISTRY_ARN = "registry:arn:123";

        private static final String ANOTHER_REGISTRY_NAME = "Unit-testing-registry-2";
        private static final String ANOTHER_REGISTRY_ARN = "registry:arn:9232";

        private static final ResourceModel MODEL = ResourceModel.builder().build();
        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(MODEL)
                .nextToken(TestData.NEXT_TOKEN)
                .build();

        public static final RegistryListItem REGISTRY_LIST_ITEM_1 =
            RegistryListItem
                .builder()
                .registryName(REGISTRY_NAME)
                .registryArn(REGISTRY_ARN)
                .status(RegistryStatus.AVAILABLE)
                .createdTime(Instant.now().toString())
                .updatedTime(Instant.now().toString())
                .build();

        public final static ResourceModel RESOURCE_MODEL_1 = ResourceModel
            .builder()
            .name(REGISTRY_NAME)
            .arn(REGISTRY_ARN)
            .build();

        public final static ResourceModel RESOURCE_MODEL_2 = ResourceModel
            .builder()
            .name(ANOTHER_REGISTRY_NAME)
            .arn(ANOTHER_REGISTRY_ARN)
            .build();

        public static final RegistryListItem REGISTRY_LIST_ITEM_2 =
            RegistryListItem
                .builder()
                .registryName(ANOTHER_REGISTRY_NAME)
                .registryArn(ANOTHER_REGISTRY_ARN)
                .status(RegistryStatus.AVAILABLE)
                .createdTime(Instant.now().toString())
                .updatedTime(Instant.now().toString())
                .build();

        public static final List<RegistryListItem> REGISTRIES = Arrays.asList(
            REGISTRY_LIST_ITEM_1,
            REGISTRY_LIST_ITEM_2
        );

        public static final Iterable<? extends ResourceModel> REGISTRIES_MODELS = Arrays.asList(
            RESOURCE_MODEL_1,
            RESOURCE_MODEL_2
        );
    }
}
