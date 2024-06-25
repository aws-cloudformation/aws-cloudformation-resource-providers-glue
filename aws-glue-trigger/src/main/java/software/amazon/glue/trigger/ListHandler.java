package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ListTriggersRequest;
import software.amazon.awssdk.services.glue.model.ListTriggersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger) {


        final ResourceModel model = request.getDesiredResourceState();

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s] Calling List Triggers", request.getStackId(),
                        request.getClientRequestToken()));

        return listTriggers(proxyClient, callbackContext, model, request, logger, proxy);
    }

    private ProgressEvent<ResourceModel, CallbackContext> listTriggers(
            final ProxyClient<GlueClient> proxyClient,
            final CallbackContext callbackContext,
            final ResourceModel model,
            final ResourceHandlerRequest<ResourceModel> request,
            final Logger logger,
            final AmazonWebServicesClientProxy proxy) {

        return proxy.initiate("AWS-Glue-Trigger::ListHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(listRequest -> Translator.translateToListRequest(request.getNextToken()))
                .makeServiceCall((awsRequest, client) -> listTriggerResponse(proxyClient, awsRequest, logger))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .done(listTriggersResponse -> ProgressEvent.<ResourceModel, CallbackContext> builder()
                                                                                            .resourceModels(Translator.translateFromListResponse(listTriggersResponse))
                                                                                            .status(OperationStatus.SUCCESS)
                                                                                            .nextToken(listTriggersResponse.nextToken())
                                                                                            .build());
    }

    private ListTriggersResponse listTriggerResponse (
            final ProxyClient<GlueClient> proxyClient,
            final ListTriggersRequest awsRequest,
            final Logger logger) {
        ListTriggersResponse response = proxyClient
                .injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::listTriggers);
        logger.log("Successfully listed all triggers.");
        return response;
    }
}
