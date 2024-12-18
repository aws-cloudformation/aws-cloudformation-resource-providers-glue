package software.amazon.glue.table;

import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;

import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {


        protected DeleteHandler() {
                super();
        }

        @VisibleForTesting
        protected DeleteHandler(final GlueClient glueClient) {
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

                // Validate the model
                if(model == null || StringUtils.isNullOrEmpty(model.getDatabaseName())) {
                return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                        "Database Name is required.");
                }

                if(StringUtils.isNullOrEmpty(model.getTableName())) {
                return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                        "Table Name is required.");
                }

                if(!TableUtils.validateLowercase(model.getDatabaseName()) && !TableUtils.validateLowercase(model.getTableName())) {
                return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                        String.format("Database name [ %s ] or Table name [ %s ] must not contain uppercase characters",
                                model.getDatabaseName(), model.getTableName()));
                }

                logger.log(String.format("[StackId: %s, ClientRequestToken: %s, DatabaseName: %s, TableName: %s Entered Delete Handler",
                        request.getStackId(), request.getClientRequestToken(), model.getDatabaseName(), model.getTableName()));

                // new
                return ProgressEvent.progress(model, callbackContext)
                        .checkExistence(request, progress -> checkExistence(proxy, request, callbackContext, proxyClient, logger, model))
                        .then(progress -> proxy.initiate("AWS-Glue-Table::DeleteHandler", proxyClient, model, callbackContext)
                                .translateToServiceRequest(Translator::translateToDeleteRequest)
                                .makeServiceCall((deleteRequest, client) -> {
                                DeleteTableResponse response = proxyClient.injectCredentialsAndInvokeV2(
                                        deleteRequest, client.client()::deleteTable);
                                logger.log(String.format("%s successfully deleted", ResourceModel.TYPE_NAME));
                                return response;
                                })
                                .handleError((erroredRequest, exception, client, resourceModel, context) -> handleError(erroredRequest,
                                        logger, exception, resourceModel, context))
                                .done(awsResponse -> ProgressEvent.defaultSuccessHandler(null))
                        );
        }

    private ProgressEvent<ResourceModel, CallbackContext> checkExistence(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger,
            final ResourceModel model
    ) {
        if(callbackContext.isPreExistenceCheckDone()) {
            return ProgressEvent.progress(model, callbackContext);
        }

        logger.log(String.format("[ClientRequestToken: %s] [StackId: %s] Entered Delete Handler (existence check)",
                request.getClientRequestToken(), request.getStackId()));

        return proxy.initiate("AWS-Glue-Table::DeleteCheckExistence", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToReadRequest)
                .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest, client.client()::getTable))
                .handleError((erroredRequest, exception, client, resourceModel, context) ->
                        handlePreExistenceCheckErrors(erroredRequest, exception, resourceModel, context, request, logger))
                .done(awsResponse -> {
                    logger.log(String.format("[ClientRequestToken: %s] Table [ %s ] already exists. " +
                                    "Failing CREATE operation. CallbackContext: %s%n",
                            request.getClientRequestToken(),
                            awsResponse.table().name(),
                            callbackContext));
                        callbackContext.setPreExistenceCheckDone(true);
                        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .callbackContext(callbackContext)
                        .resourceModel(model)
                        .status(OperationStatus.IN_PROGRESS)
                        .callbackDelaySeconds(1)
                        .build();
        });
    }

    private ProgressEvent<ResourceModel, CallbackContext> handlePreExistenceCheckErrors(
            GlueRequest glueRequest,
            Exception exception,
            ResourceModel resourceModel,
            CallbackContext callbackContext,
            ResourceHandlerRequest<ResourceModel> request,
            Logger logger
    ) {
        callbackContext.setPreExistenceCheckDone(true);

        final String errorCode = getErrorCode(exception);
        if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorCode)) {
            logger.log(String.format("[ClientRequestToken: %s] Table %s does not exist. " +
                            "Failing DELETE operation. CallbackContext: %S%n",
                    request.getClientRequestToken(),
                    resourceModel.getTableName(),
                    callbackContext));

            return ProgressEvent.failed(
                    resourceModel,
                    callbackContext,
                    HandlerErrorCode.NotFound,
                    String.format("Table with name [ %s ] not found", resourceModel.getTableName()) // why confusing? confirm with Tom
            );
        } else if (ACCESS_DENIED_EXCEPTION.equals(errorCode)) {
            logger.log(String.format("[ClientRequestToken: %s] No permissions to read Table. Returning control to" +
                            " Workflows to continue DELETE (existence check).",
                    request.getClientRequestToken()));

            callbackContext.setPreExistenceCheckDenied(true);
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .callbackContext(callbackContext)
                    .resourceModel(resourceModel)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackDelaySeconds(1)
                    .build();
        }

        return handleError(glueRequest, logger, exception, resourceModel, callbackContext);
    }
}
