package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTagsRequest;
import software.amazon.awssdk.services.glue.model.GetTagsResponse;
import software.amazon.awssdk.services.glue.model.GetTriggerRequest;
import software.amazon.awssdk.services.glue.model.GetTriggerResponse;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {

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

        if(model == null || StringUtils.isEmpty(model.getName())) {
            return ProgressEvent
                    .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, NAME_CANNOT_BE_EMPTY);
        }

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s, Name: %s] Entered Read Handler",
                request.getStackId(), request.getClientRequestToken(), model.getName()));

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> getTrigger( proxyClient, callbackContext, model, logger, proxy))
                .then(progress -> getTags( proxyClient, callbackContext, model, logger, proxy, request));
    }

    private ProgressEvent<ResourceModel, CallbackContext> getTrigger(
            final ProxyClient<GlueClient> proxyClient,
            final CallbackContext callbackContext,
            final ResourceModel model,
            final Logger logger,
            final AmazonWebServicesClientProxy proxy) {

        return proxy.initiate("AWS-Glue-Trigger::ReadHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(resourceModel.getName()))
                .makeServiceCall((getTriggerRequest, client) -> getTriggerResponse(proxyClient, model.getName(), getTriggerRequest, callbackContext))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> getTags(
            final ProxyClient<GlueClient> proxyClient,
            final CallbackContext callbackContext,
            final ResourceModel model,
            final Logger logger,
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request) {

        return proxy.initiate("AWS-Glue-Trigger::GetTagsReadHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToReadTagRequest(generateArn(request, resourceModel)))
                .makeServiceCall((getTagsRequest, client) -> getTagsResponse(proxyClient, getTagsRequest, callbackContext))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .done(awsResponse -> ProgressEvent.defaultSuccessHandler(Translator.translateFromReadResponse(callbackContext)));
    }


    private GetTriggerResponse getTriggerResponse(
            final ProxyClient<GlueClient> proxyClient,
            final String triggerName,
            final GetTriggerRequest awsRequest,
            final CallbackContext callbackContext) {

        GetTriggerResponse response = proxyClient
                .injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::getTrigger);
        logger.log(String.format("Reading Trigger %s.", triggerName));
        callbackContext.setGetTriggerResponse(response);
        return response;
    }

    private GetTagsResponse getTagsResponse(
            final ProxyClient<GlueClient> proxyClient,
            final GetTagsRequest awsRequest,
            final CallbackContext callbackContext) {

        GetTagsResponse response = proxyClient.injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::getTags);
        callbackContext.setGetTagsResponse(response);
        return response;
    }

    private String generateArn(final ResourceHandlerRequest<ResourceModel> request,
                               final ResourceModel model) {

        String regionName = request.getRegion();
        String partition = getPartition(regionName);
        return String.format("arn:%s:glue:%s:%s:trigger/%s",
                partition,
                regionName,
                request.getAwsAccountId(),
                model.getName());
    }

    private String getPartition(String regionName) {
        if (regionName.matches(".*cn.*")) {
            return "aws-cn";
        } else if (regionName.matches(".*gov.*")) {
            return "aws-us-gov";
        }
        return "aws";
    }
}
