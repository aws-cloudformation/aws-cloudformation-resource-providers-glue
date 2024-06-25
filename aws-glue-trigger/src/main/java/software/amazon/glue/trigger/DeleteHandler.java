package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.glue.model.DeleteTriggerRequest;
import software.amazon.awssdk.services.glue.model.DeleteTriggerResponse;
import software.amazon.awssdk.services.glue.model.GetTriggerRequest;
import software.amazon.awssdk.services.glue.model.GetTriggerResponse;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {

    private static final String STATUS_DELETING = "DELETING";

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

        if(model == null || StringUtils.isNullOrEmpty(model.getName())) {
            return ProgressEvent
                    .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, NAME_CANNOT_BE_EMPTY);
        }

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s] Calling Delete Trigger", request.getStackId(), request.getClientRequestToken()));

        return ProgressEvent.progress(model, callbackContext)
                .checkExistence(request, progress -> checkExistence(proxy, request, callbackContext, proxyClient, logger, model))
                .then(progress -> deleteTrigger(proxyClient, callbackContext, model, logger, proxy))
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteTrigger(
            final ProxyClient<GlueClient> proxyClient,
            final CallbackContext callbackContext,
            final ResourceModel model,
            final Logger logger,
            final AmazonWebServicesClientProxy proxy) {

        return proxy.initiate("AWS-Glue-Trigger::DeleteHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToDeleteRequest)
                .makeServiceCall((awsRequest, client) -> deleteTriggerResponse(proxyClient, awsRequest))
                .stabilize((awsRequest, awsResponse, client, resourceModel, context) -> stabilizeDelete(proxyClient, resourceModel, logger))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .progress();
    }

    private DeleteTriggerResponse deleteTriggerResponse (
            final ProxyClient<GlueClient> proxyClient,
            final DeleteTriggerRequest awsRequest) {
        return proxyClient
                .injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::deleteTrigger);
    }

    protected static Boolean stabilizeDelete(
            final ProxyClient<GlueClient> proxyClient,
            final ResourceModel model,
            final Logger logger) {
        try {
            GetTriggerRequest getTriggerRequest = GetTriggerRequest.builder()
                    .name(model.getName())
                    .build();
            GetTriggerResponse getTriggerResponse = proxyClient
                    .injectCredentialsAndInvokeV2(getTriggerRequest, proxyClient.client()::getTrigger);
            String currentState = getTriggerResponse.trigger().stateAsString();
            String name = getTriggerResponse.trigger().name();
            if (STATUS_DELETING.equals(currentState)) {
                logger.log(String.format("%s has not stabilized yet.", name));
                return false;
            }
            logger.log(String.format("%s has failed to be stabilized.", name));
            throw new Exception(INVALID_STATE_MSG + currentState);
        } catch (Exception e) {
            return handleException(e, model, logger);
        }
    }

    private static Boolean handleException(Exception e, ResourceModel model, Logger logger) {
        if (getErrorCode(e).equals(REQUEST_LIMIT_EXCEEDED)) {
            return false;
        }
        if (getErrorCode(e).equals(ENTITY_NOT_FOUND_EXCEPTION)) {
            logger.log(String.format("Successfully deleted (stabilized): %s", model.getName()));
            return true;
        }
        // Handle other exceptions if needed
        throw new RuntimeException(e);
    }

    private ProgressEvent<ResourceModel, CallbackContext> checkExistence(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger,
            final ResourceModel model
    ) {
        if (callbackContext.isDeletePreExistenceCheckDone()) {
            return ProgressEvent.progress(model, callbackContext);
        }

        logger.log(String.format("[ClientRequestToken: %s][StackId: %s] Entered Delete Handler (existence check)",
                request.getClientRequestToken(), request.getStackId()));
        return proxy.initiate("AWS-Glue-Trigger::DeleteCheckExistence", proxyClient,
                        model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(resourceModel.getName()))
                .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest,
                        client.client()::getTrigger))
                .handleError((errorRequest, exception, client, resourceModel, context) ->
                        handlePreExistenceCheckErrors(errorRequest, exception, proxyClient, resourceModel, context, request))
                .done(awsResponse -> {
                    logger.log(String.format("[ClientRequestToken: %s] Resource exists. Returning control to " +
                                    "Workflows to continue DELETE (existence check).",
                            request.getClientRequestToken()));
                    return ProgressEvent.progress(model, callbackContext);
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
        callbackContext.setDeletePreExistenceCheckDone(true);

        final String errorCode = getErrorCode(exception);
        if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorCode)) {
            logger.log(String.format("[ClientRequestToken: %s] Resource does not exists." +
                            "Failing Delete operation. CallbackContext: %s%n",
                    request.getClientRequestToken(),
                    callbackContext));

            return ProgressEvent.failed(
                    resourceModel,
                    callbackContext,
                    HandlerErrorCode.NotFound,
                    String.format("Trigger with Id [ %s ] not found", request.getDesiredResourceState().getName())
            );
        }
        return handleError(glueRequest, logger, exception, proxyClient, resourceModel, callbackContext);
    }
}
