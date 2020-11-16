package software.amazon.glue.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteRegistryRequest;
import software.amazon.awssdk.services.glue.model.DeleteRegistryResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetRegistryRequest;
import software.amazon.awssdk.services.glue.model.RegistryId;
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

import static software.amazon.glue.registry.ExceptionTranslator.translateToCfnException;

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
                proxy.initiate(
                    "AWS-Glue-Registry::Delete",
                    proxyClient,
                    progress.getResourceModel(),
                    progress.getCallbackContext()
                )
                    .translateToServiceRequest(this::fromResourceModel)
                    .backoffDelay(DELAY)
                    .makeServiceCall(this::deleteRegistry)
                    .stabilize(this::isDeleteStabilized)
                    .done(
                        awsResponse ->
                            ProgressEvent.<ResourceModel, CallbackContext>builder()
                                .status(OperationStatus.SUCCESS)
                                .build()));
    }

    private Boolean isDeleteStabilized(
        final DeleteRegistryRequest deleteRegistryRequest,
        final DeleteRegistryResponse deleteRegistryResponse,
        final ProxyClient<GlueClient> proxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext
    ) {
        final String registryName = deleteRegistryResponse.registryName();

        try {
            final GlueClient glueClient = proxyClient.client();
            final GetRegistryRequest getRegistryRequest =
                GetRegistryRequest
                    .builder()
                    .registryId(
                        RegistryId
                            .builder()
                            .registryName(registryName)
                            .build()
                    )
                    .build();

            proxyClient
                .injectCredentialsAndInvokeV2(getRegistryRequest, glueClient::getRegistry);

            return false;
        } catch (EntityNotFoundException e) {
            logger.log(
                String.format(
                    "%s [%s] successfully deleted.",
                    ResourceModel.TYPE_NAME,
                    registryName
                )
            );
            return true;
        } catch (AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format("%s [%s] deletion status couldn't be retrieved: %s",
                    ResourceModel.TYPE_NAME,
                    registryName,
                    e.getMessage()),
                e);
        }
    }

    private DeleteRegistryResponse deleteRegistry(
        final DeleteRegistryRequest deleteRegistryRequest,
        final ProxyClient<GlueClient> proxyClient) {

        DeleteRegistryResponse deleteRegistryResponse = null;

        final GlueClient glueClient = proxyClient.client();

        final String registryName =
            deleteRegistryRequest.registryId().registryName();

        try {
            deleteRegistryResponse = proxyClient.injectCredentialsAndInvokeV2(
                deleteRegistryRequest,
                glueClient::deleteRegistry
            );
        } catch (final AwsServiceException e) {
            translateToCfnException(e, registryName);
        }
        logger.log(
            String.format(
                "Requested to delete %s [%s].",
                ResourceModel.TYPE_NAME,
                registryName
            )
        );
        return deleteRegistryResponse;
    }

    private DeleteRegistryRequest fromResourceModel(final ResourceModel model) {
        return DeleteRegistryRequest
            .builder()
            .registryId(
                RegistryId
                    .builder()
                    .registryName(model.getName())
                    .build())
            .build();
    }
}
