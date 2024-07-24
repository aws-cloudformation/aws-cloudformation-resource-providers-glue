package software.amazon.glue.job;

import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.TagResourceRequest;
import software.amazon.awssdk.services.glue.model.TagResourceResponse;
import software.amazon.awssdk.services.glue.model.Job;
import software.amazon.awssdk.services.glue.model.UntagResourceRequest;
import software.amazon.awssdk.services.glue.model.UntagResourceResponse;
import software.amazon.awssdk.services.glue.model.UpdateJobRequest;
import software.amazon.awssdk.services.glue.model.UpdateJobResponse;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase{

    @Mock
    private AmazonWebServicesClientProxy proxy;


    @Mock
    private ProxyClient<GlueClient> proxyClient;

    @Mock
    GlueClient glueClient;
    private UpdateHandler handler;
    String name = "sample_job";
    String newDescription = "new description";
    String Description = "old description";
    Map<String, Object> modelDefaultArguments = new HashMap<>();

    Map<String, Object> desiredTags = new HashMap<>();
    Map<String, Object> previousTags = new HashMap<>();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        System.setProperty("aws.region", "us-east-1");
        handler = new UpdateHandler();
        glueClient = mock(glueClient.getClass());
        proxyClient = MOCK_PROXY(proxy, glueClient);
        modelDefaultArguments.put("df1", "value1");
        modelDefaultArguments.put("df2", "value2");
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


        final UpdateJobResponse updateJobResponse = UpdateJobResponse.builder()
                                .jobName(name)
                                .build();

        when(proxyClient.client().updateJob(any(UpdateJobRequest.class)))
        .thenReturn(updateJobResponse);

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
    public void handleRequest_NoInput() {

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(null, null);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NAME_CANNOT_BE_EMPTY);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
    public void handleRequest_NoName() {

        ResourceModel model = ResourceModel.builder()
                .name("")
                .description(newDescription)
                .tags(desiredTags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                model,
                generateStandardPreviousResourceModel());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(BaseHandlerStd.NAME_CANNOT_BE_EMPTY);
        assertThat(response.getErrorCode().toString()).isEqualTo(BaseHandlerStd.INVALID_REQUEST);

    }

    @Test
        public void handleRequest_EmptyDesiredTags() {

            ResourceModel model = ResourceModel.builder()
                    .name(name)
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

            final UpdateJobResponse updateJobResponse = UpdateJobResponse.builder()
                            .jobName(name)
                            .build();

            when(proxyClient.client().updateJob(any(UpdateJobRequest.class)))
                    .thenReturn(updateJobResponse);

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

            final UpdateJobResponse updateJobResponse = UpdateJobResponse.builder()
                            .jobName(name)
                            .build();

            when(proxyClient.client().updateJob(any(UpdateJobRequest.class)))
                    .thenReturn(updateJobResponse);

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

                final UpdateJobResponse updateJobResponse = UpdateJobResponse.builder()
                                .jobName(name)
                                .build();


                when(proxyClient.client().updateJob(any(UpdateJobRequest.class)))
                        .thenReturn(updateJobResponse);

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

                final UpdateJobResponse updateJobResponse = UpdateJobResponse.builder()
                                .jobName(name)
                                .build();

                when(proxyClient.client().updateJob(any(UpdateJobRequest.class)))
                        .thenReturn(updateJobResponse);

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
        void handleRequestThrottlingException_ShouldProgress() {
                ResourceModel previousModel = ResourceModel.builder()
                .name(name)
                .description(newDescription)
                .tags(Collections.emptyMap())
                .build();


                final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                        generateStandardDesiredResourceModel(),
                        previousModel);

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
                ResourceModel previousModel = ResourceModel.builder()
                .name(name)
                .description(Description)
                .tags(desiredTags)
                .build();

                final ResourceHandlerRequest<ResourceModel> request = generateResourceHandlerRequest(
                        generateStandardDesiredResourceModel(),
                        previousModel);

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
                when(proxyClient.client().updateJob(any(UpdateJobRequest.class))).thenThrow(exception);
        }


    private ResourceModel generateStandardDesiredResourceModel() {
        return ResourceModel.builder()
                .name(name)
                .description(newDescription)
                .tags(desiredTags)
                .build();
    }

    private ResourceModel generateStandardPreviousResourceModel() {
        return ResourceModel.builder()
                .name(name)
                .description(Description)
                .tags(previousTags)
                .build();
    }
}
