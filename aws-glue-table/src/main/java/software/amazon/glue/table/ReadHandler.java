package software.amazon.glue.table;

import com.google.common.annotations.VisibleForTesting;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import com.amazonaws.util.StringUtils;

public class ReadHandler extends BaseHandlerStd {

    protected ReadHandler() {
        super();
    }

    @VisibleForTesting
    protected ReadHandler(final GlueClient glueClient) {
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

        if (StringUtils.isNullOrEmpty(model.getDatabaseName()) || StringUtils.isNullOrEmpty(model.getTableName())) {
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                    String.format("Database name [ %s ], Table name [ %s ] must not contain uppercase characters", model.getDatabaseName(),
                            model.getTableName()));
        }

        if(!TableUtils.validateLowercase(model.getDatabaseName()) && !TableUtils.validateLowercase(model.getTableName())) {
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                    String.format("Database name [ %s ] or Table name [ %s ] must not contain uppercase characters",
                            model.getDatabaseName(), model.getTableName()));
        }

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s, DatabaseName: %s, TableName: %s Entered Read Handler",
                request.getStackId(), request.getClientRequestToken(), model.getDatabaseName(), model.getTableName()));

        return proxy.initiate("AWS-Glue-Table::ReadHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToReadRequest)
                .makeServiceCall((getRequest, client) -> {
                    GetTableResponse response = client.injectCredentialsAndInvokeV2(getRequest, client.client()::getTable);
                    logger.log(String.format("Reading table %s from database %s", model.getTableName(), model.getDatabaseName()));
                    return response;
                })
                .handleError((erroredRequest, exception, client, resourceModel, context) -> handleError(erroredRequest, logger, exception, resourceModel, context))
                .done(awsResponse -> {
                    logger.log(String.format("Response from GetTable request: %s", awsResponse.toString()));
                    return ProgressEvent.defaultSuccessHandler(Translator.translateFromReadResponse(awsResponse));
                });
    }
}
