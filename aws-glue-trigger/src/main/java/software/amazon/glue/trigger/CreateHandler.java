package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CreateHandler extends BaseHandlerStd {

    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger) {

        this.logger = logger;

        final ResourceModel model = request.getDesiredResourceState();

        if(model == null || StringUtils.isNullOrEmpty(model.getType()) || model.getActions().isEmpty()) {
            return ProgressEvent
                    .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, EMPTY_MODEL_OR_TYPE_ACTIONS_ERROR_MESSAGE);
        }

        if (StringUtils.isNullOrEmpty(model.getName())) {
            String resourceIdentifier = IdentifierUtils.generateResourceIdentifier(
                    request.getLogicalResourceIdentifier(),
                    request.getClientRequestToken(),
                    GENERATED_PHYSICAL_ID_MAX_LEN);
            model.setName(resourceIdentifier);
        }

        final Map<String, String> mergedTags = new HashMap<>();
        Map<String, String> convertedTags = Translator.convertObjectMapToStringMap(model.getTags());

        mergedTags.putAll(Optional.ofNullable(convertedTags).orElse(Collections.emptyMap()));
        mergedTags.putAll(Optional.ofNullable(request.getDesiredResourceTags()).orElse(Collections.emptyMap()));

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s, Name: %s] Entered Create Handler",
                request.getStackId(), request.getClientRequestToken(), model.getName()));

        return ProgressEvent.progress(model, callbackContext)
                .checkExistence(request, progress -> checkExistence(proxy, request, callbackContext, proxyClient, logger, model))
                .then(progress -> createTrigger(proxyClient, request, progress.getCallbackContext(), progress.getResourceModel(), mergedTags, model.getName(), logger, proxy));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createTrigger(
            final ProxyClient<GlueClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ResourceModel model,
            final Map<String, String> tags,
            final String name,
            final Logger logger,
            final AmazonWebServicesClientProxy proxy) {

        return proxy.initiate("AWS-Glue-Trigger::CreateHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToCreateRequest(tags, name, model))
                .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest, client.client()::createTrigger))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .done(createTriggerResponse -> {
                    model.setName(createTriggerResponse.name());
                    logger.log(String.format("Resource created in StackId: %s with name: %s",
                            request.getStackId(),
                            model.getName()));
                    return ProgressEvent.<ResourceModel, CallbackContext> builder()
                            .resourceModel(Translator.translateFromCreateResponse(createTriggerResponse))
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
    ) {
        if (callbackContext.isPreExistenceCheckDone()) {
            return ProgressEvent.progress(model, callbackContext);
        }

        logger.log(String.format("[ClientRequestToken: %s][StackId: %s] Entered Create Handler (existence check)",
                request.getClientRequestToken(), request.getStackId()));
        return proxy.initiate("AWS-Glue-Trigger::CreateCheckExistence", proxyClient,
                        model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(resourceModel.getName()))
                .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest,
                        client.client()::getTrigger))
                .handleError((errorRequest, exception, client, resourceModel, context) ->
                        handlePreExistenceCheckErrors(errorRequest, exception, proxyClient, resourceModel, context, request))
                .done(awsResponse -> {
                    logger.log(String.format("[ClientRequestToken: %s] Resource %s already exists. " +
                                    "Failing CREATE operation. CallbackContext: %s%n",
                            request.getClientRequestToken(),
                            awsResponse.trigger().name(),
                            callbackContext));
                    return ProgressEvent.failed(
                            model,
                            callbackContext,
                            HandlerErrorCode.AlreadyExists,
                            String.format("Trigger with Id [ %s ] already exists.", awsResponse.trigger().name()));
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> handlePreExistenceCheckErrors(
            final GlueRequest glueRequest,
            final Exception exception,
            final ProxyClient<GlueClient> proxyClient,
            final ResourceModel resourceModel,
            final CallbackContext callbackContext,
            final ResourceHandlerRequest<ResourceModel> request
    ) {
        callbackContext.setPreExistenceCheckDone(true);

        final String errorCode = getErrorCode(exception);
        if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorCode)) {
            logger.log(String.format("[ClientRequestToken: %s] Resource does not exist. Returning control to " +
                            "Workflows to continue CREATE (existence check).",
                    request.getClientRequestToken()));

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
