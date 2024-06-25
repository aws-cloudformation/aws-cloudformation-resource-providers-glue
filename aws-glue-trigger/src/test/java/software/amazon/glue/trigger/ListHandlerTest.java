package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ListTriggersRequest;
import software.amazon.awssdk.services.glue.model.ListTriggersResponse;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private ListHandler handler;

    final List<String> names = new ArrayList<>();
    List<ResourceModel> resourceModels = new ArrayList<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-west-2");
        handler = new ListHandler();
        glueClient = mock(glueClient.getClass());
        proxyClient = MOCK_PROXY(proxy, glueClient);

        names.add("test1");
        names.add("test2");

        resourceModels = names.stream()
                .map(name -> ResourceModel.builder().name(name).build())
                .collect(Collectors.toList());

    }

    public void tear_down() {
        verify(glueClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(glueClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

        final ListTriggersResponse listTriggersResponse = ListTriggersResponse.builder()
                .triggerNames(names)
                .build();

        when(proxyClient.client().listTriggers(any(ListTriggersRequest.class)))
                .thenReturn(listTriggersResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isEqualTo(resourceModels);
        assertThat(response.getResourceModels().size()).isEqualTo(names.size());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        tear_down();
    }

    @Test
    public void handleRequest_NoTriggers() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

        final ListTriggersResponse listTriggersResponse = ListTriggersResponse.builder()
                .triggerNames(Collections.emptyList())
                .build();

        when(proxyClient.client().listTriggers(any(ListTriggersRequest.class)))
                .thenReturn(listTriggersResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels().size()).isEqualTo(0);
        assertThat(response.getResourceModels()).isEmpty();

        tear_down();
    }

    @Test
    public void handleRequest_MoreThanOneTriggers() {
        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

        final ListTriggersResponse listTriggersResponse = ListTriggersResponse.builder()
                .triggerNames(names)
                .build();

        when(proxyClient.client().listTriggers(any(ListTriggersRequest.class)))
                .thenReturn(listTriggersResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isEqualTo(resourceModels);
        assertThat(response.getResourceModels().size()).isEqualTo(2);

        tear_down();
    }

    private ResourceModel generateStandardValidResourceModel() {
        return ResourceModel.builder()
                .build();
    }
}
