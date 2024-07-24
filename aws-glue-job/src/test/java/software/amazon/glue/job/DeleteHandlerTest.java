package software.amazon.glue.job;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteJobRequest;
import software.amazon.awssdk.services.glue.model.DeleteJobResponse;
import software.amazon.awssdk.services.glue.model.GetJobRequest;
import software.amazon.awssdk.services.glue.model.GetJobResponse;
import software.amazon.awssdk.services.glue.model.Job;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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
public class DeleteHandlerTest extends AbstractTestBase{

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private DeleteHandler handler;

    String name = "DummyJob";
    String role = "arn:aws:iam::111222333444:role/SampleGlueJob";
    String description = "dummy job for test";




    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-east-1");
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

        final GetJobResponse getJobResponse = GetJobResponse.builder()
                                                    .job(Job.builder()
                                                        .name(name)
                                                        .role(role)
                                                        .description(description)
                                                        .build())
                                                    .build();

        AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

        when(proxyClient.client().getJob(any(GetJobRequest.class)))
                .thenReturn(getJobResponse)
                .thenThrow(exception);

        final DeleteJobResponse deleteJobResponse = DeleteJobResponse.builder()
                        .jobName(name)
                        .build();

        when(proxyClient.client().deleteJob(any(DeleteJobRequest.class)))
                        .thenReturn(deleteJobResponse);


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
    public void handleRequest_NoName() {

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

            final GetJobResponse getJobResponse = GetJobResponse.builder()
                    .job(Job.builder()
                            .name(name)
                            .description(description)
                            .build())
                    .build();

            AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

            when(proxyClient.client().getJob(any(GetJobRequest.class)))
                    .thenReturn(getJobResponse)
                    .thenThrow(exception);

            final DeleteJobResponse deleteJobResponse = DeleteJobResponse.builder()
                            .jobName(name)
                            .build();

            when(proxyClient.client().deleteJob(any(DeleteJobRequest.class)))
                            .thenReturn(deleteJobResponse);

            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

            final ProgressEvent<ResourceModel, CallbackContext> secondCallResponse
                    = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

            assertThat(secondCallResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
            assertThat(secondCallResponse.getErrorCode().toString()).isEqualTo(BaseHandlerStd.NOT_FOUND);

            tear_down();

        }

        @Test
            public void handleRequest_PreExistenceCheckErrors() {

                final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

                AwsServiceException exception = exceptionCreator(BaseHandlerStd.ENTITY_NOT_FOUND_EXCEPTION);

                when(proxyClient.client().getJob(any(GetJobRequest.class)))
                        .thenThrow(exception);

                final ProgressEvent<ResourceModel, CallbackContext> response
                        = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);


                assertThat(response).isNotNull();
                assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);

                tear_down();
            }

        @Test
             void handleRequestThrottlingException_ShouldProgress() {
                 final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

                 givenproxyClientReturnsError(BaseHandlerStd.THROTTLING_EXCEPTION, 429);

                 CallbackContext context = new CallbackContext();
                 context.setPreExistenceCheckDone(true);
                 final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, logger);

                 assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
                 assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
                 assertThat(response.getCallbackDelaySeconds()).isGreaterThan(0);

             }

        @Test
             void handleRequestHttpConnectionTimeoutException_ShouldProgress() {
                 final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateResourceModel(name), null);

                 givenproxyClientReturnsError("HttpConnectionTimeoutException", 504);

                 CallbackContext context = new CallbackContext();
                 context.setPreExistenceCheckDone(true);
                 final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, logger);

                 assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
                 assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
                 assertThat(response.getCallbackDelaySeconds()).isGreaterThan(0);
             }

        private void givenproxyClientReturnsError(String errorCode, int statusCode) {
            AwsServiceException exception = AwsServiceException.builder()
                    .statusCode(statusCode)
                    .awsErrorDetails(AwsErrorDetails.builder()
                            .errorCode(errorCode)
                            .build())
                    .build();
            when(proxyClient.client().deleteJob(any(DeleteJobRequest.class))).thenThrow(exception);
        }

    private ResourceModel generateResourceModel(String name) {
        return ResourceModel.builder()
                .name(name)
                .build();
    }
}
