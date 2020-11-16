package software.amazon.glue.schema;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;
import software.amazon.awssdk.services.glue.model.UpdateSchemaRequest;
import software.amazon.awssdk.services.glue.model.UpdateSchemaResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.glue.schema.ResourceModel;
import software.amazon.glue.schema.SchemaVersion;

import static software.amazon.glue.schema.ExceptionTranslator.translateToCfnException;

public class UpdateHandler extends BaseHandlerStd {
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
                    "AWS-Glue-Schema::Update::first",
                    proxyClient,
                    progress.getResourceModel(),
                    progress.getCallbackContext())

                    .translateToServiceRequest(this::fromResourceModel)
                    .makeServiceCall(this::updateSchema)
                    //Stabilization is not required for Schema Update.
                    .stabilize((awsRequest, awsResponse, client, model, context) -> true)
                    .progress())
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private UpdateSchemaResponse updateSchema(
        final UpdateSchemaRequest updateSchemaRequest,
        final ProxyClient<GlueClient> proxyClient) {
        final GlueClient glueClient = proxyClient.client();

        UpdateSchemaResponse updateSchemaResponse = null;
        final String identifier = updateSchemaRequest.schemaId().toString();

        try {
            updateSchemaResponse =
                proxyClient.injectCredentialsAndInvokeV2(updateSchemaRequest, glueClient::updateSchema);
        } catch (AwsServiceException e) {
            translateToCfnException(e, identifier);
        }
        logger.log(
            String.format(
                "%s [%s] successfully updated.",
                ResourceModel.TYPE_NAME,
                identifier
            ));
        return updateSchemaResponse;

    }

    private UpdateSchemaRequest fromResourceModel(ResourceModel resourceModel) {
        final SchemaVersion schemaVersion = resourceModel.getCheckpointVersion();
        final Boolean isLatestVersion = schemaVersion != null ? schemaVersion.getIsLatest() : null;

        //JSON schema doesn't support Long.
        final Integer versionNumberInt = schemaVersion != null ? schemaVersion.getVersionNumber() : null;
        final Long versionNumberLong = versionNumberInt != null ? Long.valueOf(versionNumberInt) : null;

        SchemaVersionNumber schemaVersionNumber = null;
        if (isLatestVersion != null || versionNumberLong != null) {
            schemaVersionNumber =
                SchemaVersionNumber
                    .builder()
                    .versionNumber(versionNumberLong)
                    .latestVersion(isLatestVersion)
                    .build();
        }

        return
            UpdateSchemaRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(resourceModel.getArn())
                        .build()
                )
                .compatibility(resourceModel.getCompatibility())
                .description(resourceModel.getDescription())
                .schemaVersionNumber(schemaVersionNumber)
                .build();
    }
}
