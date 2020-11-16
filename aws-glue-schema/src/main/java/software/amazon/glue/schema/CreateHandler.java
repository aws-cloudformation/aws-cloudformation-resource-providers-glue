package software.amazon.glue.schema;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.glue.schema.Tag;
import software.amazon.glue.schema.ResourceModel;
import software.amazon.glue.schema.Registry;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

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
                    "AWS-Glue-Schema::Create",
                    proxyClient,
                    progress.getResourceModel(),
                    progress.getCallbackContext())

                    .translateToServiceRequest(this::fromResourceModel)
                    .makeServiceCall(this::createSchema)
                    //Stabilization not required for schema creation.
                    .stabilize((awsRequest, awsResponse, client, model, context) -> true)
                    .done(createSchemaResponse ->
                        ProgressEvent.defaultSuccessHandler(toResourceModel(createSchemaResponse)))
            );
    }

    private CreateSchemaResponse createSchema(
        final CreateSchemaRequest createSchemaRequest,
        final ProxyClient<GlueClient> proxyClient) {

        CreateSchemaResponse createSchemaResponse = null;
        final GlueClient glueClient = proxyClient.client();
        try {
            createSchemaResponse = proxyClient.injectCredentialsAndInvokeV2(
                createSchemaRequest,
                glueClient::createSchema);
        } catch (final AwsServiceException e) {
            final String identifier = createSchemaRequest.schemaName();
            ExceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "%s [%s] successfully created.",
                ResourceModel.TYPE_NAME,
                createSchemaRequest.schemaName())
        );
        return createSchemaResponse;
    }

    private CreateSchemaRequest fromResourceModel(final ResourceModel model) {
        RegistryId registryId = null;
        final Registry registry = model.getRegistry();

        if (registry != null) {
            registryId =
                RegistryId
                    .builder()
                    .registryArn(registry.getArn())
                    .registryName(registry.getName())
                    .build();
        }

        return CreateSchemaRequest
            .builder()
            .schemaName(model.getName())
            .description(model.getDescription())
            .compatibility(model.getCompatibility())
            .dataFormat(model.getDataFormat())
            .registryId(registryId)
            .schemaDefinition(model.getSchemaDefinition())
            .tags(tagsFromModel(model.getTags()))
            .build();
    }

    private ResourceModel toResourceModel(final CreateSchemaResponse createSchemaResponse) {
        return ResourceModel
            .builder()
            .arn(createSchemaResponse.schemaArn())
            .name(createSchemaResponse.schemaName())
            .description(createSchemaResponse.description())
            .dataFormat(createSchemaResponse.dataFormatAsString())
            .compatibility(createSchemaResponse.compatibilityAsString())
            .initialSchemaVersionId(createSchemaResponse.schemaVersionId())
            .tags(toResourceTags(createSchemaResponse.tags()))
            .checkpointVersion(
                SchemaVersion
                    .builder()
                    .versionNumber(createSchemaResponse.schemaCheckpoint().intValue())
                    .isLatest(
                        createSchemaResponse.schemaCheckpoint().equals(createSchemaResponse.latestSchemaVersion())
                    )
                    .build())
            .registry(
                Registry
                    .builder()
                    .arn(createSchemaResponse.registryArn())
                    .build()
            )
            .build();
    }

    private List<Tag> toResourceTags(final Map<String, String> tags) {
        return
            tags
                .entrySet()
                .stream()
                .map(entry -> new Tag(entry.getKey(), entry.getValue()))
                .collect(toList());
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
