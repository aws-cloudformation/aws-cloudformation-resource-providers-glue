package software.amazon.glue.trigger;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTagsRequest;
import software.amazon.awssdk.services.glue.model.GetTagsResponse;
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
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private ReadHandler handler;

    String name = "testTrigger";
    String type = TriggerType.ON_DEMAND.toString();
    String description = "test description";

    List<Action> modelActions = new ArrayList<>();
    List<software.amazon.awssdk.services.glue.model.Action> sdkActions = new ArrayList<>();

    Map<String, Object> modelArguments = new HashMap<>();
    Map<String, String> sdkArguments = new HashMap<>();
    Map<String, String> tags = new HashMap<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-west-2");
        handler = new ReadHandler();
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

        tags.put("key", "value");

    }

    public void tear_down() {
        verify(glueClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(glueClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {

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

        final GetTagsResponse getTagsResponse = GetTagsResponse.builder()
                .tags(tags)
                .build();

        when(proxyClient.client().getTags(any(GetTagsRequest.class)))
                .thenReturn(getTagsResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ResourceModel expectedModel = ResourceModel.builder()
                .name(name)
                .type(type)
                .actions(modelActions)
                .description(description)
                .tags(Translator.convertStringMapToObjectMap(tags))
                .build();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(expectedModel);
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

        ResourceModel model = ResourceModel.builder()
                .name("")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NAME_CANNOT_BE_EMPTY);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_WrongName() {

        ResourceModel model = generateStandardValidResourceModel();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getTrigger(any(GetTriggerRequest.class)))
                .thenThrow(exception);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.NOT_FOUND);

        tear_down();

    }

    private ResourceModel generateStandardValidResourceModel() {
        return ResourceModel.builder()
                .name(name)
                .build();
    }

}
