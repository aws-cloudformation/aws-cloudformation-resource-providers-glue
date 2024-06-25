package software.amazon.glue.trigger;

import io.netty.util.internal.StringUtil;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTriggerRequest;
import software.amazon.awssdk.services.glue.model.CreateTriggerResponse;
import software.amazon.awssdk.services.glue.model.GetTriggerRequest;
import software.amazon.awssdk.services.glue.model.GetTriggerResponse;
import software.amazon.awssdk.services.glue.model.Trigger;
import software.amazon.awssdk.services.glue.model.TriggerType;
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
import software.amazon.cloudformation.resource.IdentifierUtils;

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
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private CreateHandler handler;

    String name = "testTrigger";
    String type = TriggerType.ON_DEMAND.toString();
    String description = "test description";

    List<Action> modelActions = new ArrayList<>();
    List<software.amazon.awssdk.services.glue.model.Action> sdkActions = new ArrayList<>();

    Map<String, Object> modelArguments = new HashMap<>();
    Map<String, String> sdkArguments = new HashMap<>();
    Map<String, Object> modelTags = new HashMap<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-west-2");
        handler = new CreateHandler();
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

        modelTags.put("key1", "value1");
        modelTags.put("key2", "value2");

    }

    public void tear_down() {
        verify(glueClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(glueClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenThrow(exception);

        final CreateTriggerResponse createTriggerResponse = CreateTriggerResponse.builder()
                .name(name)
                .build();

        when(proxyClient.client().createTrigger(any(CreateTriggerRequest.class)))
                .thenReturn(createTriggerResponse);

        CallbackContext callbackContext = new CallbackContext();

        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> response1
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);


        ResourceModel expectedModel = ResourceModel.builder()
                .name(name)
                .build();

        assertThat(response1).isNotNull();
        assertThat(response1.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response1.getCallbackContext()).isNull();
        assertThat(response1.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response1.getResourceModel()).isEqualTo(expectedModel);
        assertThat(response1.getResourceModels()).isNull();
        assertThat(response1.getMessage()).isNull();
        assertThat(response1.getErrorCode()).isNull();

        tear_down();

    }

    @Test
    public void handleRequest_NullModel() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(null, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.EMPTY_MODEL_OR_TYPE_ACTIONS_ERROR_MESSAGE);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_EmptyType() {

        ResourceModel model = ResourceModel.builder()
                .name(name)
                .type("")
                .actions(modelActions)
                .description(description)
                .tags(modelTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

        CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.EMPTY_MODEL_OR_TYPE_ACTIONS_ERROR_MESSAGE);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_EmptyActions() {

        ResourceModel model = ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(Collections.emptyList())
                .description(description)
                .tags(modelTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

        CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.EMPTY_MODEL_OR_TYPE_ACTIONS_ERROR_MESSAGE);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_NoNameWithStackIDAnd() {

        ResourceModel model = ResourceModel.builder()
                .type(type)
                .actions(modelActions)
                .description(description)
                .tags(modelTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .previousResourceState(null)
                .logicalResourceIdentifier("12345")
                .clientRequestToken("67890")
                .stackId("abc")
                .build();

        String assignedName = generateName(request);

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenThrow(exception);

        final CreateTriggerResponse createTriggerResponse = CreateTriggerResponse.builder()
                .name(assignedName)
                .build();

        when(proxyClient.client().createTrigger(any(CreateTriggerRequest.class)))
                .thenReturn(createTriggerResponse);

        CallbackContext callbackContext = new CallbackContext();

        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> finalResponse
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getResourceModel().getName()).isEqualTo(assignedName);

        tear_down();

    }

    @Test
    public void handleRequest_NoName() {

        ResourceModel model = ResourceModel.builder()
                .type(type)
                .actions(modelActions)
                .description(description)
                .tags(modelTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .previousResourceState(null)
                .logicalResourceIdentifier("12345")
                .clientRequestToken("67890")
                .build();

        String assignedName = generateName(request);

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenThrow(exception);

        final CreateTriggerResponse createTriggerResponse = CreateTriggerResponse.builder()
                .name(assignedName)
                .build();

        when(proxyClient.client().createTrigger(any(CreateTriggerRequest.class)))
                .thenReturn(createTriggerResponse);

        CallbackContext callbackContext = new CallbackContext();

        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> finalResponse
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getResourceModel().getName()).isEqualTo(assignedName);

        tear_down();

    }

    @Test
    public void handleRequest_EmptyName() {
        ResourceModel model = ResourceModel.builder()
                .name("")
                .type(type)
                .actions(modelActions)
                .description(description)
                .tags(modelTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .previousResourceState(null)
                .logicalResourceIdentifier("12345")
                .clientRequestToken("67890")
                .build();

        String assignedName = generateName(request);

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenThrow(exception);

        final CreateTriggerResponse createTriggerResponse = CreateTriggerResponse.builder()
                .name(assignedName)
                .build();

        when(proxyClient.client().createTrigger(any(CreateTriggerRequest.class)))
                .thenReturn(createTriggerResponse);

        CallbackContext callbackContext = new CallbackContext();

        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> finalResponse
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getResourceModel().getName()).isEqualTo(assignedName);

        tear_down();
    }

    @Test
    public void handleRequest_AlreadyExists() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

        final GetTriggerResponse getTriggerResponse = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .build())
                .build();

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenReturn(getTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.ALREADY_EXISTS);

        tear_down();

    }


    private ResourceModel generateStandardValidResourceModel() {
        return ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(modelActions)
                .description(description)
                .tags(modelTags)
                .build();
    }

    private String generateName(ResourceHandlerRequest<ResourceModel> request) {
        String name = StringUtil.EMPTY_STRING;
        if (request.getStackId() != null && !request.getStackId().isEmpty()) {
            name = IdentifierUtils.generateResourceIdentifier(request.getStackId(),
                    request.getLogicalResourceIdentifier(), request.getClientRequestToken(), BaseHandlerStd.GENERATED_PHYSICAL_ID_MAX_LEN);
        } else if ((request.getStackId() == null || request.getStackId().isEmpty())) {
            name = IdentifierUtils.generateResourceIdentifier(
                    request.getLogicalResourceIdentifier(), request.getClientRequestToken(), BaseHandlerStd.GENERATED_PHYSICAL_ID_MAX_LEN);
        }
        return name;
    }
}
