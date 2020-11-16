package software.amazon.glue.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateRegistryRequest;
import software.amazon.awssdk.services.glue.model.CreateRegistryResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static software.amazon.glue.registry.ExceptionTranslator.translateToCfnException;

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
                    "AWS-Glue-Registry::Create",
                    proxyClient,
                    progress.getResourceModel(),
                    progress.getCallbackContext())

                    .translateToServiceRequest(this::fromResourceModel)
                    .makeServiceCall(this::createRegistry)
                    .stabilize((awsRequest, awsResponse, client, model, context) -> true)
                    .done(awsResponse -> ProgressEvent.defaultSuccessHandler(toResourceModel(awsResponse))
                    ));
    }

    private CreateRegistryResponse createRegistry(
        CreateRegistryRequest createRegistryRequest,
        ProxyClient<GlueClient> proxyClient) {

        CreateRegistryResponse createRegistryResponse = null;
        try {
            GlueClient glueClient = proxyClient.client();
            createRegistryResponse = proxyClient.injectCredentialsAndInvokeV2(
                createRegistryRequest,
                glueClient::createRegistry
            );
        } catch (final AwsServiceException e) {
            translateToCfnException(e, createRegistryRequest.registryName());
        }

        logger.log(
            String.format("%s [%s] successfully created.",
                ResourceModel.TYPE_NAME,
                createRegistryRequest.registryName()
            )
        );
        return createRegistryResponse;
    }

    private CreateRegistryRequest fromResourceModel(final ResourceModel model) {
        return CreateRegistryRequest
            .builder()
            .registryName(model.getName())
            .description(model.getDescription())
            .tags(tagsFromModel(model.getTags()))
            .build();
    }

    private ResourceModel toResourceModel(final CreateRegistryResponse createRegistryResponse) {
        return
            ResourceModel
                .builder()
                .name(createRegistryResponse.registryName())
                .arn(createRegistryResponse.registryArn())
                .description(createRegistryResponse.description())
                .build();
    }

    private Map<String, String> tagsFromModel(final List<Tag> tags) {
        if (tags == null) {
            return Collections.emptyMap();
        }
        return tags
            .stream()
            .collect(toMap(Tag::getKey, Tag::getValue));
    }
}
