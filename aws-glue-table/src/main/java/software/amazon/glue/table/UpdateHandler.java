package software.amazon.glue.table;

import com.google.common.annotations.VisibleForTesting;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import com.google.common.collect.Sets;

public class UpdateHandler extends BaseHandlerStd {

        protected UpdateHandler() {
                super();
        }

        @VisibleForTesting
        protected UpdateHandler(final GlueClient glueClient) {
                super(glueClient);
        }

    static final String TABLE_NAME_CANNOT_BE_EMPTY = "Model validation failed. Required key TableName cannot be empty.";
    static final String DATABASE_NAME_CANNOT_BE_EMPTY = "Model validation failed. Required key DatabaseName cannot be empty.";

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        logger.log("Received request to update Glue table");

        final ResourceModel model = request.getDesiredResourceState();

        if(model == null || StringUtils.isEmpty(model.getTableInput().getName())) {
            return ProgressEvent
                    .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, TABLE_NAME_CANNOT_BE_EMPTY);
        }

        if(StringUtils.isEmpty(model.getDatabaseName())) {
            return ProgressEvent
                    .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, DATABASE_NAME_CANNOT_BE_EMPTY);
        }

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s] Entered Update Handler", request.getStackId(), request.getClientRequestToken()));

        return proxy.initiate("AWS-Glue-Table::UpdateHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToUpdateRequest(model))
                .makeServiceCall((updateRequest, client) -> {
                    logger.log(String.format("Updating Table %s", model.getTableInput().getName()));
                    UpdateTableResponse response = client.injectCredentialsAndInvokeV2(updateRequest,
                            client.client()::updateTable);
                    logger.log(String.format("Resource updated in StackId: %s with TableName: %s",
                            request.getStackId(),
                            model.getTableInput().getName()));
                    return response;
                })
                .handleError((erroredRequest, exception, client, resourceModel,context) ->
                        handleError(erroredRequest, logger, exception, resourceModel, context))
                .done(awsResponse -> ProgressEvent.defaultSuccessHandler(Translator.translateFromUpdateResponse(awsResponse, model)));
    }
}
