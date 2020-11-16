package software.amazon.glue.schemaversion;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteSchemaVersionsRequest;
import software.amazon.awssdk.services.glue.model.DeleteSchemaVersionsResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.cloudformation.proxy.OperationStatus;

import java.time.Duration;

import static software.amazon.glue.schemaversion.ExceptionTranslator.translateToCfnException;

public class DeleteHandler extends BaseHandlerStd {
    private Logger logger;
    private static final Delay DELAY =
        Constant.of()
            .timeout(Duration.ofSeconds(120L))
            .delay(Duration.ofSeconds(4L))
            .build();

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {
        this.logger = logger;

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress ->
                proxy.initiate("AWS-Glue-SchemaVersion::Delete", proxyClient, progress.getResourceModel(),
                    progress.getCallbackContext())
                    .translateToServiceRequest(this::getSchemaVersionRequest)
                    .backoffDelay(DELAY)
                    .makeServiceCall(this::deleteSchemaVersion)
                    .stabilize(this::isDeleteStabilized)
                    .done(
                        awsResponse ->
                            ProgressEvent.<ResourceModel, CallbackContext>builder()
                                .status(OperationStatus.SUCCESS)
                                .build()));
    }

    private Boolean isDeleteStabilized(
        final GetSchemaVersionRequest getSchemaVersionRequest,
        final DeleteSchemaVersionsResponse deleteSchemaVersionsResponse,
        final ProxyClient<GlueClient> proxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext) {

        final String versionId = resourceModel.getVersionId();
        try {
            final GlueClient glueClient = proxyClient.client();

            proxyClient
                .injectCredentialsAndInvokeV2(
                    getSchemaVersionRequest,
                    glueClient::getSchemaVersion
                );

            logger.log(
                String.format("%s [%s] is not deleted yet",
                    ResourceModel.TYPE_NAME,
                    versionId
                )
            );

            return false;
        } catch (EntityNotFoundException e) {
            logger.log(
                String.format("%s [%s] successfully deleted.",
                    ResourceModel.TYPE_NAME,
                    versionId
                ));
            return true;
        } catch (AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format("%s [%s] deletion status couldn't be retrieved: %s",
                    ResourceModel.TYPE_NAME,
                    versionId,
                    e.getMessage()),
                e);
        }
    }

    /**
     * HACK: We only have DeleteSchemaVersions API that takes
     * schema version number as input. We need to fetch the version number first by using
     * the versionId.
     *
     * @param getSchemaVersionRequest {@link GetSchemaVersionRequest}.
     * @param client                  ProxyClient
     * @return DeleteSchemaVersionsResponse
     */
    private DeleteSchemaVersionsResponse deleteSchemaVersion(
        final GetSchemaVersionRequest getSchemaVersionRequest,
        final ProxyClient<GlueClient> client) {

        final GlueClient glueClient = client.client();
        final String identifier = getSchemaVersionRequest.schemaVersionId();

        DeleteSchemaVersionsResponse deleteSchemaVersionsResponse = null;
        try {
            final GetSchemaVersionResponse getSchemaVersionResponse =
                client.injectCredentialsAndInvokeV2(
                    getSchemaVersionRequest, glueClient::getSchemaVersion);

            final Long versionNumber = getSchemaVersionResponse.versionNumber();

            logger.log(
                String.format(
                    "Fetched version number %s for %s [%s]",
                    versionNumber,
                    ResourceModel.TYPE_NAME,
                    identifier
                )
            );

            final DeleteSchemaVersionsRequest deleteSchemaVersionsRequest =
                deleteSchemaVersionRequest(
                    Schema
                        .builder()
                        .schemaArn(getSchemaVersionResponse.schemaArn())
                        .build(),
                    versionNumber
                );

            deleteSchemaVersionsResponse =
                client.injectCredentialsAndInvokeV2(deleteSchemaVersionsRequest, glueClient::deleteSchemaVersions);

        } catch (final AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "Requested to delete %s [%s].",
                ResourceModel.TYPE_NAME,
                identifier
            )
        );
        return deleteSchemaVersionsResponse;
    }

    private GetSchemaVersionRequest getSchemaVersionRequest(final ResourceModel model) {
        return GetSchemaVersionRequest
            .builder()
            .schemaVersionId(model.getVersionId())
            .build();
    }

    private DeleteSchemaVersionsRequest deleteSchemaVersionRequest(
        final Schema schemaId,
        final Long versionNumber) {
        return DeleteSchemaVersionsRequest
            .builder()
            .versions(String.valueOf(versionNumber))
            .schemaId(
                SchemaId
                    .builder()
                    .schemaArn(schemaId.getSchemaArn())
                    .schemaName(schemaId.getSchemaName())
                    .registryName(schemaId.getRegistryName())
                    .build()
            )
            .build();
    }
}
