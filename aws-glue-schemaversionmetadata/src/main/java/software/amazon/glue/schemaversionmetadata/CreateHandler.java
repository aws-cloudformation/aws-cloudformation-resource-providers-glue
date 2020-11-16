package software.amazon.glue.schemaversionmetadata;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.glue.schemaversionmetadata.ExceptionTranslator.translateToCfnException;

public class CreateHandler extends BaseHandlerStd {
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
                proxy.initiate(
                    "AWS-Glue-SchemaVersionMetadata::Create",
                    proxyClient,
                    progress.getResourceModel(),
                    progress.getCallbackContext())

                    .translateToServiceRequest(this::fromResourceModel)
                    .makeServiceCall(this::createSchemaVersionMetadata)
                    //Stabilization not required for this resource.
                    .stabilize((awsRequest, awsResponse, client, model, context) -> true)
                    .done(createSchemaVersionMetadataResponse ->
                        ProgressEvent.defaultSuccessHandler(toResourceModel(createSchemaVersionMetadataResponse)))
            );
    }

    private ResourceModel toResourceModel(
        final PutSchemaVersionMetadataResponse putSchemaVersionMetadataResponse) {
        return
            ResourceModel
                .builder()
                .schemaVersionId(putSchemaVersionMetadataResponse.schemaVersionId())
                .key(putSchemaVersionMetadataResponse.metadataKey())
                .value(putSchemaVersionMetadataResponse.metadataValue())
                .build();
    }

    private PutSchemaVersionMetadataResponse createSchemaVersionMetadata(
        final PutSchemaVersionMetadataRequest putSchemaVersionMetadataRequest,
        ProxyClient<GlueClient> proxyClient) {

        final String identifier =
            getIdentifier(
                putSchemaVersionMetadataRequest.schemaVersionId(),
                putSchemaVersionMetadataRequest.metadataKeyValue().metadataKey(),
                putSchemaVersionMetadataRequest.metadataKeyValue().metadataValue()
            );

        PutSchemaVersionMetadataResponse putSchemaVersionMetadataResponse = null;
        final GlueClient glueClient = proxyClient.client();
        try {
            putSchemaVersionMetadataResponse = proxyClient.injectCredentialsAndInvokeV2(
                putSchemaVersionMetadataRequest,
                glueClient::putSchemaVersionMetadata);
        } catch (final AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "%s [%s] successfully created.",
                ResourceModel.TYPE_NAME,
                identifier
            )
        );
        return putSchemaVersionMetadataResponse;

    }

    private PutSchemaVersionMetadataRequest fromResourceModel(final ResourceModel resourceModel) {
        return PutSchemaVersionMetadataRequest
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
