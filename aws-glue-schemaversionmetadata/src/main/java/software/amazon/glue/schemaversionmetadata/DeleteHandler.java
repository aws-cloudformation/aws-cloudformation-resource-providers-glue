package software.amazon.glue.schemaversionmetadata;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.RemoveSchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.RemoveSchemaVersionMetadataResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.OperationStatus;

import static software.amazon.glue.schemaversionmetadata.ExceptionTranslator.translateToCfnException;

public class DeleteHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress ->
                proxy.initiate("AWS-Glue-SchemaVersionMetadata::Delete", proxyClient, progress.getResourceModel(),
                    progress.getCallbackContext())
                    .translateToServiceRequest(this::fromResourceModel)
                    .makeServiceCall(this::removeSchemaVersionMetadata)
                    .stabilize((awsRequest, awsResponse, client, model, context) -> true)
                    .done(
                        awsResponse ->
                            ProgressEvent.<ResourceModel, CallbackContext>builder()
                                .status(OperationStatus.SUCCESS)
                                .build()));
    }

    private RemoveSchemaVersionMetadataResponse removeSchemaVersionMetadata(
        final RemoveSchemaVersionMetadataRequest removeSchemaVersionMetadataRequest,
        final ProxyClient<GlueClient> proxyClient) {

        final GlueClient glueClient = proxyClient.client();
        final String identifier =
            getIdentifier(
                removeSchemaVersionMetadataRequest.schemaVersionId(),
                removeSchemaVersionMetadataRequest.metadataKeyValue().metadataKey(),
                removeSchemaVersionMetadataRequest.metadataKeyValue().metadataValue()
            );

        RemoveSchemaVersionMetadataResponse removeSchemaVersionMetadataResponse = null;
        try {
            removeSchemaVersionMetadataResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    removeSchemaVersionMetadataRequest, glueClient::removeSchemaVersionMetadata);

        } catch (final AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "%s [%s] successfully deleted.",
                ResourceModel.TYPE_NAME,
                identifier
            )
        );
        return removeSchemaVersionMetadataResponse;
    }

    private RemoveSchemaVersionMetadataRequest fromResourceModel(
        final ResourceModel resourceModel) {
        return
            RemoveSchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(resourceModel.getSchemaVersionId())
                .metadataKeyValue(
                    MetadataKeyValuePair
                        .builder()
                        .metadataKey(resourceModel.getKey())
                        .metadataValue(resourceModel.getValue())
                        .build()
                )
                .build();
    }
}
