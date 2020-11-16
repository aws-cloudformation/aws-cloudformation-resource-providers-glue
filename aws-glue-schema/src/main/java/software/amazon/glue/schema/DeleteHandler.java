package software.amazon.glue.schema;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.DeleteSchemaResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.glue.schema.ResourceModel;
import software.amazon.cloudformation.proxy.OperationStatus;

import java.time.Duration;

import static software.amazon.glue.schema.ExceptionTranslator.translateToCfnException;

public class DeleteHandler extends BaseHandlerStd {
    private static final Delay DELAY =
        Constant.of()
            .timeout(Duration.ofSeconds(120L))
            .delay(Duration.ofSeconds(2L))
            .build();

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
                proxy.initiate("AWS-Glue-Schema::Delete", proxyClient, progress.getResourceModel(),
                    progress.getCallbackContext())
                    .translateToServiceRequest(this::fromResourceModel)
                    .backoffDelay(DELAY)
                    .makeServiceCall(this::deleteSchema)
                    .stabilize(this::isDeleteStabilized)
                    .done(
                        awsResponse ->
                            ProgressEvent.<ResourceModel, CallbackContext>builder()
                                .status(OperationStatus.SUCCESS)
                                .build()));
    }

    private Boolean isDeleteStabilized(
        final DeleteSchemaRequest deleteSchemaRequest,
        final DeleteSchemaResponse deleteSchemaResponse,
        final ProxyClient<GlueClient> proxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext) {
        final String schemaArn = deleteSchemaResponse.schemaArn();

        try {
            final GlueClient glueClient = proxyClient.client();
            final GetSchemaRequest getSchemaRequest =
                GetSchemaRequest
                    .builder()
                    .schemaId(
                        SchemaId
                            .builder()
                            .schemaArn(schemaArn)
                            .build()
                    )
                    .build();

            proxyClient
                .injectCredentialsAndInvokeV2(getSchemaRequest, glueClient::getSchema);

            return false;
        } catch (EntityNotFoundException e) {
            logger.log(
                String.format(
                    "%s [%s] successfully deleted.",
                    ResourceModel.TYPE_NAME,
                    schemaArn
                )
            );
            return true;
        } catch (AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format("%s [%s] deletion status couldn't be retrieved: %s",
                    ResourceModel.TYPE_NAME,
                    schemaArn,
                    e.getMessage()),
                e);
        }
    }

    private DeleteSchemaResponse deleteSchema(
        final DeleteSchemaRequest deleteSchemaRequest,
        final ProxyClient<GlueClient> client) {

        DeleteSchemaResponse deleteSchemaResponse = null;
        final GlueClient glueClient = client.client();
        final String identifier = deleteSchemaRequest.schemaId().toString();

        try {
            deleteSchemaResponse =
                client.injectCredentialsAndInvokeV2(deleteSchemaRequest, glueClient::deleteSchema);

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
        return deleteSchemaResponse;
    }

    private DeleteSchemaRequest fromResourceModel(final ResourceModel model) {
        final String schemaArn =
            model.getArn();

        return DeleteSchemaRequest
            .builder()
            .schemaId(
                SchemaId
                    .builder()
                    .schemaArn(schemaArn)
                    .build()
            )
            .build();
    }
}
