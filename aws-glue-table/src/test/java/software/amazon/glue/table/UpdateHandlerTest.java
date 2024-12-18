package software.amazon.glue.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.glue.table.CallbackContext;
import software.amazon.glue.table.ResourceModel;
import software.amazon.glue.table.TableInput;
import software.amazon.glue.table.Translator;
import software.amazon.glue.table.UpdateHandler;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;

    private UpdateHandler handler;

    @BeforeEach
    public void setUp() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        glueClient = mock(glueClient.getClass());
        handler = new UpdateHandler(glueClient);
        proxyClient = MOCK_PROXY(proxy, glueClient);
    }

    @Test
    public void handleRequest_Success() {
        // Arrange
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableInput(TableInput.builder().name("test-table").build())
                .build();

        final ResourceModel newModel = ResourceModel.builder()
        .databaseName("test-database")
        .catalogId("catalog_id")
        .tableInput(TableInput.builder().name("test-table").build())
        .build();


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();

        final UpdateTableResponse updateTableResponse = UpdateTableResponse.builder().build();

        when(proxyClient.client().updateTable(any(UpdateTableRequest.class)))
                .thenReturn(updateTableResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(request.getDesiredResourceState().getCatalogId()).isEqualTo("catalog_id");
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequestMissingTableName_ShouldFail() {
        // Arrange
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableInput(TableInput.builder().name("").build())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequestNullModel_ShouldFail() {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(null)
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequestMissingTableInput_ShouldFail() {
        // Arrange
        final ResourceModel model = null;

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequestInternalServiceError_ShouldFail() {
        // Arrange
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableInput(TableInput.builder().name("test-table").build())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();

        final AwsServiceException internalServerException = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(BaseHandlerStd.INTERNAL_SERVICE_EXCEPTION)
                        .build())
                .build();

        when(proxyClient.client().updateTable((any(UpdateTableRequest.class))))
                .thenThrow(internalServerException);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }
}
