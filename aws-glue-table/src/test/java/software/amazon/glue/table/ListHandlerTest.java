package software.amazon.glue.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.glue.table.CallbackContext;
import software.amazon.glue.table.ResourceModel;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;

    private ListHandler handler;

    @BeforeEach
    public void setUp() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        glueClient = mock(glueClient.getClass());
        handler = new ListHandler(glueClient);
        proxyClient = MOCK_PROXY(proxy, glueClient);
    }

    @Test
    public void handleRequest_Success() {
        final ResourceModel model = new ResourceModel();

        final CallbackContext callbackContext = new CallbackContext();

        final GetTablesResponse listTablesResponse = GetTablesResponse.builder().tableList(Collections.emptyList()).build();
        when(proxyClient.client().getTables(any(GetTablesRequest.class))).thenReturn(listTablesResponse);

        // Act
        final ProgressEvent<ResourceModel, CallbackContext> result = handler.handleRequest(proxy, ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model).build(), callbackContext, proxyClient, logger);

        // Assert
        assertThat(result.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(result.getResourceModels()).isNotNull();
        verify(glueClient, times(1)).getTables(any(GetTablesRequest.class));
    }

    @Test
    void testHandleRequest_WithNextToken() {
        // Arrange
        final String nextToken = "nextToken";
        final ResourceModel model = new ResourceModel();
        final CallbackContext callbackContext = new CallbackContext();
        final GetTablesResponse listTablesResponse = GetTablesResponse.builder().nextToken(nextToken).tableList(Collections.emptyList()).build();
        when(proxyClient.client().getTables(any(GetTablesRequest.class))).thenReturn(listTablesResponse);

        // Act
        final ProgressEvent<ResourceModel, CallbackContext> result = handler.handleRequest(proxy, ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model).nextToken(nextToken).build(), callbackContext, proxyClient, logger);

        // Assert
        assertThat(result.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(result.getResourceModels()).isNotNull();
        assertThat(result.getNextToken()).isEqualTo(nextToken);
        verify(glueClient, times(1)).getTables(any(GetTablesRequest.class));
    }

    @Test
    void testHandleRequest_Error() {
        // Arrange
        final ResourceModel model = new ResourceModel();
        final CallbackContext callbackContext = new CallbackContext();
        when(proxyClient.client().getTables(any(GetTablesRequest.class))).thenThrow(RuntimeException.class);

        // Act
        final ProgressEvent<ResourceModel, CallbackContext> result = handler.handleRequest(proxy, ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model).build(), callbackContext, proxyClient, logger);

        // Assert
        assertThat(result.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(result.getResourceModels()).isNull();
        verify(glueClient, times(1)).getTables(any(GetTablesRequest.class));
    }

    @Test
    void handleRequestNoDatabases_ShouldReturnEmpty() {
        final ResourceModel model = ResourceModel.builder().build();

        List<Table> tables = new ArrayList<>();

        final CallbackContext callbackContext = new CallbackContext();

        final GetTablesResponse listTablesResponse = GetTablesResponse.builder().tableList(tables).build();

        when(proxyClient.client().getTables(any(GetTablesRequest.class))).thenReturn(listTablesResponse);

        // Act
        final ProgressEvent<ResourceModel, CallbackContext> result = handler.handleRequest(proxy, ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model).build(), callbackContext, proxyClient, logger);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(result.getCallbackContext()).isNull();
        assertThat(result.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(result.getResourceModel()).isNull();
        assertThat(result.getResourceModels()).isEmpty();
        assertThat(result.getMessage()).isNull();
        assertThat(result.getErrorCode()).isNull();
    }

    @Test
    public void handleRequestInternalServiceError_ShouldFail() {
        final ResourceModel model = ResourceModel.builder().build();

        final CallbackContext callbackContext = new CallbackContext();

        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(BaseHandlerStd.INTERNAL_SERVICE_EXCEPTION)
                        .build())
                .build();

        when(proxyClient.client().getTables(any(GetTablesRequest.class)))
                .thenThrow(exception);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model).build(), callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }
}
