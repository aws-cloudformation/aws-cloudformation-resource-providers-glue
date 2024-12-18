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
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
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
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;

    private DeleteHandler handler;

    @BeforeEach
    public void setUp() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        glueClient = mock(glueClient.getClass());
        handler = new DeleteHandler(glueClient);
        proxyClient = MOCK_PROXY(proxy, glueClient);
    }

    @Test
    public void handleRequest_DeleteTable_Success() {
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableName("test-table")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final GetTableResponse getTableResponse = GetTableResponse.builder()
                .table(Table.builder().name(model.getTableName()).build())
                .build();

        when(proxyClient.client().getTable(any(GetTableRequest.class)))
                .thenReturn(getTableResponse);

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> responsePreCheck =
                handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(responsePreCheck.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(responsePreCheck.getResourceModel()).isNotNull();
        assertThat(callbackContext.isPreExistenceCheckDone());

        final DeleteTableResponse deleteTableResponse = DeleteTableResponse.builder()
                .build();

        when(proxyClient.client().deleteTable(any(DeleteTableRequest.class)))
                .thenReturn(deleteTableResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(null);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }


    @Test
    public void handleRequest_TableDoesNotExist_ShouldFail() {
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableName("test-table")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("EntityNotFoundException")
                        .build())
                .build();

        when(proxyClient.client().getTable(any(GetTableRequest.class)))
                .thenThrow(exception);

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequest_MissingTableName_ShouldFail() {
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableName("")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequest_MissingDatabaseName_ShouldFail() {
        final ResourceModel model = ResourceModel.builder()
                .databaseName("")
                .tableName("test-table")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequestNullModel_ShouldFail() {
        final ResourceModel model = null;

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    @Test
    public void handleRequestUnauthorizedExceptionPreCheck_ShouldReturnInProgress() {
        final ResourceModel model = ResourceModel.builder()
                .databaseName("test-database")
                .tableName("test-table")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final AwsServiceException exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(BaseHandlerStd.ACCESS_DENIED_EXCEPTION)
                        .build())
                .build();

        when(proxyClient.client().getTable(any(GetTableRequest.class)))
                .thenThrow(exception);

        final CallbackContext callbackContext = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext().isPreExistenceCheckDenied()).isTrue();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(1);
        assertThat(response.getResourceModel()).isEqualTo(model);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
