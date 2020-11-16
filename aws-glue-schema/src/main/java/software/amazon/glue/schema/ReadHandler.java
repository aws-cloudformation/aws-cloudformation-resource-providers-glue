package software.amazon.glue.schema;

import software.amazon.awssdk.services.glue.GlueClient;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.glue.schema.ExceptionTranslator.translateToCfnException;

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
            "AWS-Glue-Schema::Read",
            proxyClient,
            request.getDesiredResourceState(),
            callbackContext)
            .translateToServiceRequest(this::fromResourceModel)

            .makeServiceCall(this::getSchemaResponseModel)
            .done(responseModel -> ProgressEvent.defaultSuccessHandler(responseModel));
    }

    private ResourceModel getSchemaResponseModel(
        final GetSchemaRequest getSchemaRequest,
        final ProxyClient<GlueClient> proxyClient) {

        GetSchemaResponse getSchemaResponse = null;
        GetSchemaVersionResponse getSchemaVersionResponse = null;
        final String identifier = getSchemaRequest.schemaId().toString();

        try {
            final GlueClient glueClient = proxyClient.client();

            getSchemaResponse = proxyClient.injectCredentialsAndInvokeV2(getSchemaRequest, glueClient::getSchema);
            GetSchemaVersionRequest getSchemaVersionRequest = getInitialSchemaVersionRequest(getSchemaResponse.schemaArn());

            getSchemaVersionResponse = proxyClient.injectCredentialsAndInvokeV2(
                getSchemaVersionRequest,
                glueClient::getSchemaVersion);

        } catch (final AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "%s [%s] has successfully been read.",
                ResourceModel.TYPE_NAME,
                identifier
            )
        );
        return toResourceModel(getSchemaResponse, getSchemaVersionResponse.schemaVersionId());
    }

    private GetSchemaVersionRequest getInitialSchemaVersionRequest(
        final String schemaArn) {

        return GetSchemaVersionRequest
            .builder()
            .schemaId(
                SchemaId
                    .builder()
                    .schemaArn(schemaArn)
                    .build()
            )
            .schemaVersionNumber(
                SchemaVersionNumber
                    .builder()
                    .versionNumber(1L)
                    .build()
            )
            .build();
    }

    private GetSchemaRequest fromResourceModel(final ResourceModel model) {
        return GetSchemaRequest
            .builder()
            .schemaId(
                SchemaId
                    .builder()
                    .schemaArn(model.getArn())
                    .build()
            )
            .build();
    }

    private ResourceModel toResourceModel(final GetSchemaResponse getSchemaResponse, final String initialSchemaVersionId) {
        return ResourceModel
            .builder()
            .arn(getSchemaResponse.schemaArn())
            .name(getSchemaResponse.schemaName())
            .description(getSchemaResponse.description())
            .dataFormat(getSchemaResponse.dataFormatAsString())
            .compatibility(getSchemaResponse.compatibilityAsString())
            .initialSchemaVersionId(initialSchemaVersionId)
            .checkpointVersion(
                SchemaVersion
                    .builder()
                    .versionNumber(getSchemaResponse.schemaCheckpoint().intValue())
                    .isLatest(getSchemaResponse.schemaCheckpoint().equals(getSchemaResponse.latestSchemaVersion()))
                    .build())
            .registry(
                Registry
                    .builder()
                    .arn(getSchemaResponse.registryArn())
                    .build()
            )
            .build();
    }
}
