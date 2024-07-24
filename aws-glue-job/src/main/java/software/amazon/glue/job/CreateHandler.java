package software.amazon.glue.job;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import com.google.common.base.Objects;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateJobRequest;
import software.amazon.awssdk.services.glue.model.CreateJobResponse;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import com.amazonaws.util.StringUtils;


public class CreateHandler extends BaseHandlerStd {


    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger
            ) {


        final ResourceModel model = request.getDesiredResourceState();

        if (model == null || StringUtils.isNullOrEmpty(model.getRole()) || Objects.equal(model.getCommand(), null)){
            return ProgressEvent
            .failed(model, callbackContext, HandlerErrorCode.InvalidRequest,NO_NAME_OR_ROLE_OR_COMMAND_ERROR_MESSAGE);

        }

        if (StringUtils.isNullOrEmpty(model.getName())) {
            String resourceIdentifier = IdentifierUtils.generateResourceIdentifier(
                    request.getLogicalResourceIdentifier(),
                    request.getClientRequestToken(),
                    GENERATED_PHYSICAL_ID_MAX_LEN);
            model.setName(resourceIdentifier);
        }

        logger.log(String.format("[StackId: %s, Name: %s] Entered Create Handler",
                request.getStackId(),request.getClientRequestToken(), model.getName()));

        final Map<String, String> consolidatedTags = new HashMap<>();
        Map<String, String> convertedTags = Translator.convertObjectMapToStringMap(model.getTags());

        consolidatedTags.putAll(Optional.ofNullable(convertedTags).orElse(Collections.emptyMap()));
        consolidatedTags.putAll(Optional.ofNullable(request.getDesiredResourceTags()).orElse(Collections.emptyMap()));

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s, Name: %s] Entered Create Handler",
                request.getStackId(), request.getClientRequestToken(), model.getName()));


        return ProgressEvent.progress(model, callbackContext)
                             .checkExistence(request, progress -> checkExistence(proxy, request, callbackContext, proxyClient, logger, model))
                             .then(progress -> createJob(proxyClient, request, progress.getCallbackContext(), progress.getResourceModel(), consolidatedTags, model.getName(), logger, proxy));
                 }

    private ProgressEvent<ResourceModel, CallbackContext> createJob(
        final ProxyClient<GlueClient> proxyClient,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ResourceModel model,
        final Map<String, String> tags,
        final String name,
        final Logger logger,
        AmazonWebServicesClientProxy proxy
    ){
        return proxy.initiate("AWS-Glue-Job::CreateHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToCreateRequest(tags, name, model))
                .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest, client.client()::createJob))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .done(createJobResponse -> {
                    model.setName(createJobResponse.name());
                    logger.log(String.format("Resource created in StackId: %s with name: %s",
                            request.getStackId(),
                            model.getName()));
                    return ProgressEvent.<ResourceModel, CallbackContext> builder()
                            .resourceModel(Translator.translateFromCreateResponse(createJobResponse))
                            .status(OperationStatus.SUCCESS)
                            .build();
                });
    }


    private ProgressEvent<ResourceModel, CallbackContext> checkExistence(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger,
        final ResourceModel model
    ){
        if (callbackContext.isPreExistenceCheckDone()){
            return ProgressEvent.progress(model, callbackContext);

        }
        logger.log(String.format("[ClientRequestToken: %s][StackId: %s] Create Handler Existence Check", request.getClientRequestToken(), request.getStackId()));
        return proxy.initiate("AWS-Glue-Job::CreateCheckExistence", proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(resourceModel.getName()))
                .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest, client.client()::getJob))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handlePreExistenceCheckErrors(errorRequest, exception, proxyClient, resourceModel, context, request, logger))
                .done(awsResponse -> {
                    logger.log(String.format("[ClientRequestToken: %s] Resource %s already exists" + "failing CREATE operation, CallbackContext: %s%n",
                        request.getClientRequestToken(),
                        awsResponse.job().name(),
                        callbackContext));
                    return ProgressEvent.failed(
                        model,
                        callbackContext,
                        HandlerErrorCode.AlreadyExists,
                        String.format("Resource already exists: %s", awsResponse.job().name()));

                });
            }

        private ProgressEvent<ResourceModel, CallbackContext> handlePreExistenceCheckErrors(
            final GlueRequest glueRequest,
            final Exception exception,
            final ProxyClient<GlueClient> proxyClient,
            final ResourceModel resourceModel,
            final CallbackContext callbackContext,
            final ResourceHandlerRequest<ResourceModel> request,
            final Logger logger
        ){
            callbackContext.setPreExistenceCheckDone(true);

            final String errorCode = getErrorCode(exception);

            if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorCode)){
                logger.log(String.format("[ClientRequestToke: %n] Resource does not exist." , request.getClientRequestToken()));
             return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .callbackContext(callbackContext)
                    .resourceModel(resourceModel)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackDelaySeconds(1)
                    .build();

            }
                return handleError(glueRequest, logger, exception, proxyClient, resourceModel, callbackContext);
            }
    }
