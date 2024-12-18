package software.amazon.glue.table;

import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.RandomStringUtils;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Random;

public class CreateHandler extends BaseHandlerStd {

    private static final int GENERATED_PHYSICALID_MAXLEN = 40;
    private static final int GUID_LENGTH = 12;

    protected CreateHandler() {
        super();
    }

    @VisibleForTesting
    protected CreateHandler(final GlueClient glueClient) {
        super(glueClient);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger) {


        final ResourceModel model = request.getDesiredResourceState();

        // validate model
        if (model == null || model.getTableInput() == null) {
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                    String.format("Table input is required"));
        }

        if (StringUtils.isNullOrEmpty(model.getDatabaseName()) || !TableUtils.validateLowercase(model.getDatabaseName())) {
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                    String.format("Database name [ %s ] must not be null or empty and cannot contain uppercase character", model.getDatabaseName()));
        }

        if(StringUtils.isNullOrEmpty(model.getTableInput().getName())) {
            model.setTableName(
                    generatePhysicalResourceId(
                            request.getLogicalResourceIdentifier(), request.getClientRequestToken().toLowerCase()
                    )
            );
            model.setTableInput(TableInput.builder()
                            .name(model.getTableName())
                            .build());
        } else {
            if(!TableUtils.validateLowercase(model.getTableInput().getName())) {
                return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest,
                        String.format("Table name [ %s ] must not contain uppercase characters",
                                model.getDatabaseName(), model.getTableInput().getName()));
            } else {
                model.setTableName(model.getTableInput().getName());
            }
        }

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s, DatabaseName: %s, TableName: %s Entered Create Handler",
                request.getStackId(), request.getClientRequestToken(), model.getDatabaseName(), model.getTableName()));

        // check if table exists
        return ProgressEvent.progress(model, callbackContext)
                .checkExistence(request, progress -> checkExistence(proxy, request, callbackContext, proxyClient, logger, model))
                .then(progress -> proxy.initiate("AWS-Glue-Table::CreateHandler", proxyClient, model, callbackContext)
                        .translateToServiceRequest(Translator::translateToCreateRequest)
                        .makeServiceCall((createRequest, client) -> {
                            CreateTableResponse response = client.injectCredentialsAndInvokeV2(createRequest, client.client()::createTable);
                            logger.log(String.format("Creating table with name [ %s ] in database [ %s ]", model.getTableName(), model.getDatabaseName()));
                            return response;
                        })
                        .handleError((erroredRequest, exception, client, resourceModel, context) ->
                                handleError(erroredRequest, logger, exception, resourceModel, context))
                        .done(awsResponse -> {
                            logger.log(String.format("Resource created in StackId: %s with Table Name: %s", request.getStackId(), model.getTableInput().getName()));
                            return ProgressEvent.success(model, callbackContext);
                        })
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

        logger.log(String.format("[ClientRequestToken: %s] [StackId: %s] Entered Create Handler (existence check)",
                request.getClientRequestToken(), request.getStackId()));

        return proxy.initiate("AWS-Glue-Table::CreateCheckExistence", proxyClient, model, callbackContext)
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
                    return ProgressEvent.failed(
                            model,
                            callbackContext,
                            HandlerErrorCode.AlreadyExists,
                            String.format("Table already exists: [ %s ]", awsResponse.table().name()));
                });
    }

    private ProgressEvent<ResourceModel, CallbackContext> handlePreExistenceCheckErrors(
            GetTableRequest glueRequest,
            Exception exception,
            ResourceModel resourceModel,
            CallbackContext callbackContext,
            ResourceHandlerRequest<ResourceModel> request,
            Logger logger
    ) {
        callbackContext.setPreExistenceCheckDone(true);

        final String errorCode = getErrorCode(exception);
        if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorCode)) {
            logger.log(String.format("[ClientRequestToken: %s] Table does not exist. Returning control to " +
                            "Workflows to continue CREATE (existence check).",
                    request.getClientRequestToken()));

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .callbackContext(callbackContext)
                    .resourceModel(resourceModel)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackDelaySeconds(1)
                    .build();
        } else if (ACCESS_DENIED_EXCEPTION.equals(errorCode)) {
            logger.log(String.format("[ClientRequestToken: %s] No permissions to describe resolver. Returning control to" +
                            " Workflows to continue CREATE (existence check).",
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

    private String generatePhysicalResourceId(final String logicalResourceId, final String clientRequestToken) {
        int maxLogicalIdLength = GENERATED_PHYSICALID_MAXLEN - (GUID_LENGTH + 1);
        int endIndex = Math.min(logicalResourceId.length(), maxLogicalIdLength);
        StringBuilder sb = new StringBuilder();
        if(endIndex > 0) {
            sb.append(logicalResourceId.substring(0, endIndex)).append("-");
        }
        return sb.append(
                RandomStringUtils.random(
                        GUID_LENGTH, 0, 0, true, true, null,
                        new Random(clientRequestToken.hashCode()))
        ).toString().toLowerCase();
    }
}
