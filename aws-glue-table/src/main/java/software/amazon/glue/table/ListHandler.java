package software.amazon.glue.table;

import com.google.common.annotations.VisibleForTesting;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;

public class ListHandler extends BaseHandlerStd {

    protected ListHandler() {
        super();
    }

    @VisibleForTesting
    protected ListHandler(final GlueClient glueClient) {
        super(glueClient);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        logger.log(String.format("{StackId: %s, RequestId: %s} Entered List Handler", request.getStackId(), request.getClientRequestToken()));

        return proxy.initiate("AWS-Glue-Table::ListHandler",proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToListRequest(request.getNextToken(), model.getDatabaseName()))
                .makeServiceCall((listRequest, client) -> proxyClient.injectCredentialsAndInvokeV2(listRequest, proxyClient.client()::getTables))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, resourceModel, context))
                .done((sdkRequest, listResponse, client, resourceModel, context) -> ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModels(Translator.translateFromListResponse(listResponse, model.getDatabaseName()))
                        .status(OperationStatus.SUCCESS)
                        .nextToken(listResponse.nextToken())
                        .build());
    }
}
