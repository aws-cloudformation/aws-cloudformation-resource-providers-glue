package software.amazon.glue.trigger;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteTriggerRequest;
import software.amazon.awssdk.services.glue.model.DeleteTriggerResponse;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private DeleteHandler handler;

    String name = "testTrigger";
    String type = TriggerType.ON_DEMAND.toString();
    String description = "test description";
    List<software.amazon.awssdk.services.glue.model.Action> sdkActions = new ArrayList<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-west-2");
        handler = new DeleteHandler();
        glueClient = mock(glueClient.getClass());
        proxyClient = MOCK_PROXY(proxy, glueClient);
    }

    public void tear_down() {
        verify(glueClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(glueClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

        final GetTriggerResponse getTriggerResponse = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .build())
                .build();

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenReturn(getTriggerResponse)
                .thenThrow(exception);

        final DeleteTriggerResponse deleteTriggerResponse = DeleteTriggerResponse.builder()
                .name(name)
                .build();

        when(proxyClient.client().deleteTrigger(any(DeleteTriggerRequest.class)))
                .thenReturn(deleteTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();
    }

    @Test
    public void handleRequest_NullModel() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(null, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NAME_CANNOT_BE_EMPTY);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_NoName() {

        ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NAME_CANNOT_BE_EMPTY);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_EmptyName() {

        ResourceModel model = generateResourceModel("");

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NAME_CANNOT_BE_EMPTY);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_NotFound() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

        final GetTriggerResponse getTriggerResponse = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .build())
                .build();

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenReturn(getTriggerResponse)
                .thenThrow(exception);

        final DeleteTriggerResponse deleteTriggerResponse = DeleteTriggerResponse.builder()
                .name(name)
                .build();

        when(proxyClient.client().deleteTrigger(any(DeleteTriggerRequest.class)))
                .thenReturn(deleteTriggerResponse);

        handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> secondCallResponse
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(secondCallResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(secondCallResponse.getErrorCode().toString()).isEqualTo(BaseHandlerStd.NOT_FOUND);

        tear_down();

    }

    @Test
    public void handleRequest_Stabilize() {
        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

        final GetTriggerResponse getTriggerResponseCREATED = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .state("CREATED")
                        .build())
                .build();

        final GetTriggerResponse getTriggerResponseDELETING = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .state("DELETING")
                        .build())
                .build();

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenReturn(getTriggerResponseCREATED)
                .thenReturn(getTriggerResponseDELETING)
                .thenThrow(exception);

        final DeleteTriggerResponse deleteTriggerResponse = DeleteTriggerResponse.builder()
                .name(name)
                .build();

        when(proxyClient.client().deleteTrigger(any(DeleteTriggerRequest.class)))
                .thenReturn(deleteTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> firstCallResponse
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> secondCallResponse
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(firstCallResponse).isNotNull();
        assertThat(firstCallResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(secondCallResponse.getStatus()).isEqualTo(OperationStatus.FAILED);

        tear_down();

    }

    @Test
    public void handleRequest_StabilizeInvalidState() {
        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

        final GetTriggerResponse getTriggerResponseInvalid = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .state("DUMMY_STATE")
                        .build())
                .build();

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenReturn(getTriggerResponseInvalid);

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode().toString()).isEqualTo("NotStabilized");
    }

    @Test
    public void handleRequest_PreExistenceCheckErrors() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenThrow(exception);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);

        tear_down();
    }

    @Test
    public void handleRequest_DeletePreExistenceCheckDone() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setDeletePreExistenceCheckDone(true);

        final GetTriggerResponse getTriggerResponse = GetTriggerResponse.builder()
                .trigger(Trigger.builder()
                        .name(name)
                        .type(type)
                        .actions(sdkActions)
                        .description(description)
                        .build())
                .build();

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenReturn(getTriggerResponse)
                .thenThrow(exception);

        final DeleteTriggerResponse deleteTriggerResponse = DeleteTriggerResponse.builder()
                .name(name)
                .build();

        when(proxyClient.client().deleteTrigger(any(DeleteTriggerRequest.class)))
                .thenReturn(deleteTriggerResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);
        final ProgressEvent<ResourceModel, CallbackContext> response2
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response2).isNotNull();
        assertThat(response2.getStatus()).isEqualTo(OperationStatus.SUCCESS);

        tear_down();
    }

    private ResourceModel generateResourceModel(String name) {
        return ResourceModel.builder()
                .name(name)
                .build();
    }
}
