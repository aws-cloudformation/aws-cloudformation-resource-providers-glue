package software.amazon.glue.registry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateRegistryRequest;
import software.amazon.awssdk.services.glue.model.CreateRegistryResponse;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    GlueClient sdkClient;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        sdkClient = mock(GlueClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
        handler = new CreateHandler();
    }

    @Test
    public void handleRequest_RegistryCreationSucceeds_WhenTagsAreNotPresent() {
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_REGISTRY_REQUEST_WITHOUT_TAGS,
            sdkClient::createRegistry)
        ).thenReturn(TestData.CREATE_REGISTRY_RESPONSE_WITHOUT_TAGS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITHOUT_TAGS,
            new CallbackContext(), proxyClient, logger
        );

        final ResourceModel expectedResponseModel = ResourceModel
            .builder()
            .name(TestData.REGISTRY_NAME)
            .description(TestData.REGISTRY_DESC)
            .arn(TestData.REGISTRY_ARN)
            .build();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(expectedResponseModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ReturnsSuccessfulResponse_OnRegistryCreation() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_REGISTRY_REQUEST_WITH_TAGS,
            sdkClient::createRegistry)
        ).thenReturn(TestData.CREATE_REGISTRY_RESPONSE_WITH_TAGS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
            new CallbackContext(), proxyClient, logger
        );

        final ResourceModel expectedResponseModel = ResourceModel
            .builder()
            .name(TestData.REGISTRY_NAME)
            .description(TestData.REGISTRY_DESC)
            .arn(TestData.REGISTRY_ARN)
            .build();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(expectedResponseModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ThrowsException_WhenRegistryExists() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_REGISTRY_REQUEST_WITH_TAGS,
            sdkClient::createRegistry)
        ).thenThrow(AlreadyExistsException.class);

        Exception exception = assertThrows(CfnAlreadyExistsException.class, () ->
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
                new CallbackContext(), proxyClient, logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Resource of type 'AWS::Glue::Registry' with identifier 'unit-test-registry' already exists.");
    }

    @Test
    public void handleRequest_ThrowsException_WhenAccessDenied() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_REGISTRY_REQUEST_WITH_TAGS,
            sdkClient::createRegistry)
        ).thenThrow(AccessDeniedException.class);

        Exception exception = assertThrows(CfnAccessDeniedException.class, () ->
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
                new CallbackContext(), proxyClient, logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Access denied for operation 'AWS::Glue::Registry'");
    }

    @Test
    public void handleRequest_ThrowsException_WhenResourceLimitExceeded() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_REGISTRY_REQUEST_WITH_TAGS,
            sdkClient::createRegistry)
        ).thenThrow(ResourceNumberLimitExceededException.class);

        Exception exception = assertThrows(CfnServiceLimitExceededException.class, () ->
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
                new CallbackContext(), proxyClient, logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Limit exceeded for resource of type 'AWS::Glue::Registry'. Reason:");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String REGISTRY_DESC = "Unit testing registry creation.";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";
        public final static List<Tag> RESOURCE_TAGS =
            ImmutableList.of(new Tag("Project", "Example"), new Tag("Org", "ABC"));
        public final static Map<String, String> TAGS = ImmutableSortedMap.of("Project", "Example", "Org", "ABC");

        public final static ResourceModel RESOURCE_MODEL_WITH_TAGS = ResourceModel
            .builder()
            .name(REGISTRY_NAME)
            .description(REGISTRY_DESC)
            .tags(RESOURCE_TAGS)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_TAGS =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_TAGS)
                .build();

        public final static CreateRegistryRequest CREATE_REGISTRY_REQUEST_WITH_TAGS =
            CreateRegistryRequest.builder()
                .registryName(REGISTRY_NAME)
                .description(REGISTRY_DESC)
                .tags(TAGS)
                .build();

        public final static ResourceModel RESOURCE_MODEL_WITHOUT_TAGS = ResourceModel
            .builder()
            .name(REGISTRY_NAME)
            .description(REGISTRY_DESC)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITHOUT_TAGS =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITHOUT_TAGS)
                .build();

        public final static CreateRegistryRequest CREATE_REGISTRY_REQUEST_WITHOUT_TAGS =
            CreateRegistryRequest.builder()
                .registryName(REGISTRY_NAME)
                .description(REGISTRY_DESC)
                .tags(Collections.unmodifiableMap(Collections.emptyMap()))
                .build();

        public final static CreateRegistryResponse CREATE_REGISTRY_RESPONSE_WITHOUT_TAGS =
            CreateRegistryResponse.builder()
                .registryName(TestData.REGISTRY_NAME)
                .description(TestData.REGISTRY_DESC)
                .registryArn(TestData.REGISTRY_ARN)
                .tags(Collections.emptyMap())
                .build();

        public final static CreateRegistryResponse CREATE_REGISTRY_RESPONSE_WITH_TAGS =
            CreateRegistryResponse.builder()
                .registryName(TestData.REGISTRY_NAME)
                .description(TestData.REGISTRY_DESC)
                .registryArn(TestData.REGISTRY_ARN)
                .tags(TestData.TAGS)
                .build();

    }
}
