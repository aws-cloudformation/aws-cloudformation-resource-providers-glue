package software.amazon.glue.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.UpdateRegistryRequest;
import software.amazon.awssdk.services.glue.model.UpdateRegistryResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.glue.registry.ExceptionTranslator.translateToCfnException;

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
                    "AWS-Glue-Registry::Update",
                    proxyClient,
                    progress.getResourceModel(),
                    progress.getCallbackContext())

                    .translateToServiceRequest(this::fromResourceModel)
                    .makeServiceCall(this::updateRegistry)
                    //No stabilization required for Update.
                    .stabilize((awsRequest, awsResponse, client, model, context) -> true)
                    .progress())
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private UpdateRegistryResponse updateRegistry(
        final UpdateRegistryRequest awsRequest,
        final ProxyClient<GlueClient> proxyClient) {
        UpdateRegistryResponse updateRegistryResponse = null;
        GlueClient glueClient = proxyClient.client();
        try {
            updateRegistryResponse = proxyClient.injectCredentialsAndInvokeV2(awsRequest, glueClient::updateRegistry);
        } catch (final AwsServiceException e) {
            translateToCfnException(e, awsRequest.registryId().registryName());
        }

        logger.log(String.format("%s has successfully been updated.", ResourceModel.TYPE_NAME));
        return updateRegistryResponse;
    }

    private UpdateRegistryRequest fromResourceModel(final ResourceModel model) {
        return UpdateRegistryRequest
            .builder()
            .registryId(
                RegistryId
                    .builder()
                    .registryName(model.getName())
                    .build())
            .description(model.getDescription())
            .build();
    }
}
