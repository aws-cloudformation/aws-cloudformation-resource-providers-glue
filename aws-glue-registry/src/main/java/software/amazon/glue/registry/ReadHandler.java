package software.amazon.glue.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetRegistryRequest;
import software.amazon.awssdk.services.glue.model.GetRegistryResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.glue.registry.ExceptionTranslator.translateToCfnException;

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
            "AWS-Glue-Registry::Read",
            proxyClient,
            request.getDesiredResourceState(),
            callbackContext)

            .translateToServiceRequest(this::fromResourceModel)
            .makeServiceCall(this::getRegistry)
            .done(awsResponse -> ProgressEvent.defaultSuccessHandler(toResourceModel(awsResponse)));
    }

    private GetRegistryResponse getRegistry(
        final GetRegistryRequest getRegistryRequest,
        final ProxyClient<GlueClient> proxyClient) {
        GetRegistryResponse getRegistryResponse = null;
        final String registryName = getRegistryRequest.registryId().registryName();

        try {

            GlueClient glueClient = proxyClient.client();
            getRegistryResponse = proxyClient.injectCredentialsAndInvokeV2(getRegistryRequest, glueClient::getRegistry);
        } catch (final AwsServiceException e) {
            translateToCfnException(e, registryName);
        }

        logger.log(
            String.format(
                "%s [%s] has successfully been read. ",
                ResourceModel.TYPE_NAME,
                registryName
            )
        );
        return getRegistryResponse;
    }

    private GetRegistryRequest fromResourceModel(final ResourceModel model) {
        RegistryId registryId =
            RegistryId
                .builder()
                .registryName(model.getName())
                .build();

        return GetRegistryRequest
            .builder()
            .registryId(registryId)
            .build();
    }

    private ResourceModel toResourceModel(final GetRegistryResponse getRegistryResponse) {
        return ResourceModel
            .builder()
            .name(getRegistryResponse.registryName())
            .arn(getRegistryResponse.registryArn())
            .description(getRegistryResponse.description())
            .build();
    }
}
