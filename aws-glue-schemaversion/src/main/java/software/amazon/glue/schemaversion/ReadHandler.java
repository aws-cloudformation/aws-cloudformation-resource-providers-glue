package software.amazon.glue.schemaversion;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.glue.schemaversion.ExceptionTranslator.translateToCfnException;

public class ReadHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate(
            "AWS-Glue-SchemaVersion::Read",
            proxyClient,
            request.getDesiredResourceState(),
            callbackContext)

            .translateToServiceRequest(this::fromResourceModel)
            .makeServiceCall(this::getSchemaVersion)
            .done(response ->
                ProgressEvent.defaultSuccessHandler(toResourceModel(response)));
    }

    private GetSchemaVersionResponse getSchemaVersion(
        final GetSchemaVersionRequest request,
        final ProxyClient<GlueClient> proxyClient) {

        GetSchemaVersionResponse getSchemaVersionResponse = null;
        String identifier = "";

        final GlueClient glueClient = proxyClient.client();

        try {
            getSchemaVersionResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    request,
                    glueClient::getSchemaVersion
                );
            identifier = getSchemaVersionResponse.schemaVersionId();
        } catch (final AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        logger.log(
            String.format("%s [%s] has successfully been read.",
                ResourceModel.TYPE_NAME,
                identifier
            ));
        return getSchemaVersionResponse;
    }

    private GetSchemaVersionRequest fromResourceModel(ResourceModel resourceModel) {
        return GetSchemaVersionRequest
            .builder()
            .schemaVersionId(resourceModel.getVersionId())
            .build();
    }

    private ResourceModel toResourceModel(final GetSchemaVersionResponse getSchemaVersionResponse) {
        return ResourceModel
            .builder()
            .schema(
                Schema
                    .builder()
                    .schemaArn(getSchemaVersionResponse.schemaArn())
                    .build()
            )
            .schemaDefinition(getSchemaVersionResponse.schemaDefinition())
            .versionId(getSchemaVersionResponse.schemaVersionId())
            .build();
    }

}
