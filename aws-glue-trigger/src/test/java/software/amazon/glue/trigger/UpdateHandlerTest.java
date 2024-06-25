package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.TagResourceRequest;
import software.amazon.awssdk.services.glue.model.TagResourceResponse;
import software.amazon.awssdk.services.glue.model.Trigger;
import software.amazon.awssdk.services.glue.model.TriggerType;
import software.amazon.awssdk.services.glue.model.UntagResourceRequest;
import software.amazon.awssdk.services.glue.model.UntagResourceResponse;
import software.amazon.awssdk.services.glue.model.UpdateTriggerRequest;
import software.amazon.awssdk.services.glue.model.UpdateTriggerResponse;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private UpdateHandler handler;

    String name = "testTrigger";
    String type = TriggerType.ON_DEMAND.toString();
    String oldDescription = "test description (old)";
    String newDescription = "test description (new)";

    List<Action> modelActions = new ArrayList<>();
    List<software.amazon.awssdk.services.glue.model.Action> sdkActions = new ArrayList<>();

    Map<String, Object> modelArguments = new HashMap<>();
    Map<String, String> sdkArguments = new HashMap<>();

    Map<String, Object> desiredTags = new HashMap<>();
    Map<String, Object> previousTags = new HashMap<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-west-2");
        handler = new UpdateHandler();
        glueClient = mock(glueClient.getClass());
        proxyClient = MOCK_PROXY(proxy, glueClient);


        modelArguments.put("argument1", "argumentValue1");
        modelArguments.put("argument2", "argumentValue2");
        Action modelAction = Action.builder().arguments(modelArguments).build();
        modelActions.add(modelAction);

        sdkArguments.put("argument1", "argumentValue1");
        sdkArguments.put("argument2", "argumentValue2");
        software.amazon.awssdk.services.glue.model.Action sdkAction = software.amazon.awssdk.services.glue.model.Action.builder().arguments(sdkArguments).build();
        sdkActions.add(sdkAction);

        desiredTags.put("key1", "desired");
        previousTags.put("key2", "previous");

    }

    public void tear_down() {
        verify(glueClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(glueClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                generateStandardDesiredResourceModel(),
                generateStandardPreviousResourceModel());

        final TagResourceResponse tagResourceResponse = TagResourceResponse.builder()
                .build();

        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
                .thenReturn(tagResourceResponse);

        final UntagResourceResponse untagResourceResponse = UntagResourceResponse.builder()
                .build();

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenReturn(untagResourceResponse);

        final UpdateTriggerResponse updateTriggerResponse = UpdateTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(newDescription)
                        .build())
                .build();

        when(proxyClient.client().updateTrigger(any(UpdateTriggerRequest.class)))
                .thenReturn(updateTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();
    }

    @Test
    public void handleRequest_SimpleSuccessCN() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                generateStandardDesiredResourceModel(),
                generateStandardPreviousResourceModel(),
                "cn-northwest-1");

        final TagResourceResponse tagResourceResponse = TagResourceResponse.builder()
                .build();

        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
                .thenReturn(tagResourceResponse);

        final UntagResourceResponse untagResourceResponse = UntagResourceResponse.builder()
                .build();

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenReturn(untagResourceResponse);

        final UpdateTriggerResponse updateTriggerResponse = UpdateTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(newDescription)
                        .build())
                .build();

        when(proxyClient.client().updateTrigger(any(UpdateTriggerRequest.class)))
                .thenReturn(updateTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();
    }

    @Test
    public void handleRequest_SimpleSuccessGov() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                generateStandardDesiredResourceModel(),
                generateStandardPreviousResourceModel(),
                "us-gov-east-1");

        final TagResourceResponse tagResourceResponse = TagResourceResponse.builder()
                .build();

        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
                .thenReturn(tagResourceResponse);

        final UntagResourceResponse untagResourceResponse = UntagResourceResponse.builder()
                .build();

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenReturn(untagResourceResponse);

        final UpdateTriggerResponse updateTriggerResponse = UpdateTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(newDescription)
                        .build())
                .build();

        when(proxyClient.client().updateTrigger(any(UpdateTriggerRequest.class)))
                .thenReturn(updateTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();
    }

    @Test
    public void handleRequest_NullModel() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(null, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.EMPTY_MODEL_OR_NAME_ACTIONS_ERROR_MESSAGE);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_EmptyName() {

        ResourceModel model = ResourceModel.builder()
                .name("")
                .type(type)
                .actions(modelActions)
                .description(newDescription)
                .tags(desiredTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                model,
                generateStandardPreviousResourceModel());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.EMPTY_MODEL_OR_NAME_ACTIONS_ERROR_MESSAGE);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_EmptyActions() {

        ResourceModel model = ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(Collections.emptyList())
                .description(newDescription)
                .tags(desiredTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                model,
                generateStandardPreviousResourceModel());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.EMPTY_MODEL_OR_NAME_ACTIONS_ERROR_MESSAGE);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_EmptyDesiredTags() {

        ResourceModel model = ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(modelActions)
                .description(newDescription)
                .tags(Collections.emptyMap())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                model,
                generateStandardPreviousResourceModel());

        final UntagResourceResponse untagResourceResponse = UntagResourceResponse.builder()
                .build();

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenReturn(untagResourceResponse);

        final UpdateTriggerResponse updateTriggerResponse = UpdateTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(newDescription)
                        .build())
                .build();

        when(proxyClient.client().updateTrigger(any(UpdateTriggerRequest.class)))
                .thenReturn(updateTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getTags().size()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();

    }

    @Test
    public void handleRequest_EmptyPreviousTags() {

        ResourceModel previousModel = ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(modelActions)
                .description(newDescription)
                .tags(Collections.emptyMap())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                generateStandardDesiredResourceModel(),
                previousModel);

        final TagResourceResponse tagResourceResponse = TagResourceResponse.builder()
                .build();

        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
                .thenReturn(tagResourceResponse);

        final UntagResourceResponse untagResourceResponse = UntagResourceResponse.builder()
                .build();

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenReturn(untagResourceResponse);

        final UpdateTriggerResponse updateTriggerResponse = UpdateTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(newDescription)
                        .build())
                .build();

        when(proxyClient.client().updateTrigger(any(UpdateTriggerRequest.class)))
                .thenReturn(updateTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getTags().size()).isEqualTo(desiredTags.size());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();

    }

    private ResourceModel generateStandardDesiredResourceModel() {
        return ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(modelActions)
                .description(newDescription)
                .tags(desiredTags)
                .build();
    }

    private ResourceModel generateStandardPreviousResourceModel() {
        return ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(modelActions)
                .description(oldDescription)
                .tags(previousTags)
                .build();
    }
}
