package software.amazon.glue.job;
import io.netty.util.internal.StringUtil;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateJobRequest;
import software.amazon.awssdk.services.glue.model.CreateJobResponse;
import software.amazon.awssdk.services.glue.model.GetJobRequest;
import software.amazon.awssdk.services.glue.model.GetJobResponse;
import software.amazon.awssdk.services.glue.model.Job;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import org.json.JSONObject;
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

    String name = "samplejob1";
    String role = "arn:aws:iam::111222333444:role/samplejob1";
    JobCommand modelJobCommand = new JobCommand();

    Map<String, Object> modelTags = new HashMap<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-east-1");
        handler = new CreateHandler();
        glueClient = mock(glueClient.getClass());
        proxyClient = MOCK_PROXY(proxy, glueClient);

        modelJobCommand = JobCommand.builder()
                                                .name("job1")
                                                .pythonVersion("3")
                                                .scriptLocation("s3://test/sample_glue_job.py")
                                                .build();




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

        when(proxyClient.client().getJob(any(GetJobRequest.class)))
                .thenThrow(exception);

        final CreateJobResponse createJobResponse = CreateJobResponse.builder()
                .name(name)
                .build();

        when(proxyClient.client().createJob(any(CreateJobRequest.class)))
                .thenReturn(createJobResponse);

        CallbackContext callbackContext = new CallbackContext();

        handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);


        ResourceModel expectedModel = ResourceModel.builder()
                .name(name)
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
        public void handleRequest_NoCommand() {

            ResourceModel model = ResourceModel.builder()
                    .name(name)
                    .role(role)
                    .tags(modelTags)
                    .build();

            final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

            CallbackContext callbackContext = new CallbackContext();

            final ProgressEvent<ResourceModel, CallbackContext> response
                    = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

            assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
            assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NO_NAME_OR_ROLE_OR_COMMAND_ERROR_MESSAGE);
            assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

        }

   @Test
        public void handleRequest_NoRole() {

            ResourceModel model = ResourceModel.builder()
                    .name(name)
                    .role("")
                    .tags(modelTags)
                    .build();

            final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

            CallbackContext callbackContext = new CallbackContext();

            final ProgressEvent<ResourceModel, CallbackContext> response
                    = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

            assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
            assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NO_NAME_OR_ROLE_OR_COMMAND_ERROR_MESSAGE);
            assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

        }

   @Test
        public void handleRequest_NoName() {

                ResourceModel model = ResourceModel.builder()
                        .name("")
                        .role(role)
                        .tags(modelTags)
                        .build();

                final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(model, null);

                CallbackContext callbackContext = new CallbackContext();

                final ProgressEvent<ResourceModel, CallbackContext> response
                        = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

                assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
                assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NO_NAME_OR_ROLE_OR_COMMAND_ERROR_MESSAGE);
                assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

        }

   @Test
       public void handleRequest_AlreadyExists() {

           final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

           final GetJobResponse getJobResponse = GetJobResponse.builder()
                   .job(Job.builder()
                           .name(name)
                           .role(role)
                           .build())
                   .build();

           when(proxyClient.client().getJob(any(GetJobRequest.class)))
                   .thenReturn(getJobResponse);

           final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

           assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
           assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.ALREADY_EXISTS);

           tear_down();

       }

   @Test
        void handleRequestThrottlingException_ShouldProgress() {
                final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

                givenproxyClientReturnsError(BaseHandlerStd.THROTTLING_EXCEPTION, 429);

                CallbackContext context = new CallbackContext();
                context.setPreExistenceCheckDone(true);
                final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, logger);

                assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
                assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
                assertThat(response.getCallbackDelaySeconds()).isGreaterThan(0);

                tear_down();
        }

     @Test
        void handleRequestHttpConnectionTimeoutException_ShouldProgress() {
                final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(generateStandardValidResourceModel(), null);

                givenproxyClientReturnsError("HttpConnectionTimeoutException", 504);

                CallbackContext context = new CallbackContext();
                context.setPreExistenceCheckDone(true);
                final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, logger);

                assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
                assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
                assertThat(response.getCallbackDelaySeconds()).isGreaterThan(0);

                tear_down();
        }

        private void givenproxyClientReturnsError(String errorCode, int statusCode) {
                AwsServiceException exception = AwsServiceException.builder()
                        .statusCode(statusCode)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(errorCode)
                                .build())
                        .build();
                when(proxyClient.client().createJob(any(CreateJobRequest.class))).thenThrow(exception);
        }

   private ResourceModel generateStandardValidResourceModel() {
            return ResourceModel.builder()
                    .name(name)
                    .command(modelJobCommand)
                    .role(role)
                    .build();
        }


}
