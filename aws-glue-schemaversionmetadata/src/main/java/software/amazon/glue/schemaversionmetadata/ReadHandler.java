package software.amazon.glue.schemaversionmetadata;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.Map;

import static software.amazon.glue.schemaversionmetadata.ExceptionTranslator.translateToCfnException;

public class ReadHandler extends BaseHandlerStd {
    private static final int ONLY_ELEMENT = 0;
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate(
            "AWS-Glue-SchemaVersionMetadata::Read",
            proxyClient,
            request.getDesiredResourceState(),
            callbackContext)

            .translateToServiceRequest(this::fromResourceModel)
            .makeServiceCall(this::querySchemaVersionMetadata)
            .done(querySchemaVersionMetadataResponse ->
                ProgressEvent.defaultSuccessHandler(toResourceModel(querySchemaVersionMetadataResponse)));
    }

    private QuerySchemaVersionMetadataResponse querySchemaVersionMetadata(
        final QuerySchemaVersionMetadataRequest querySchemaVersionMetadataRequest,
        ProxyClient<GlueClient> proxyClient) {
        QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse = null;

        final GlueClient glueClient = proxyClient.client();
        final MetadataKeyValuePair metadataKeyValuePair =
            querySchemaVersionMetadataRequest.metadataList().get(ONLY_ELEMENT);

        final String schemaVersionId = querySchemaVersionMetadataRequest.schemaVersionId();
        final String metadataKey = metadataKeyValuePair.metadataKey();
        final String metadataValue = metadataKeyValuePair.metadataValue();
        final String identifier = getIdentifier(schemaVersionId, metadataKey, metadataValue);

        try {
            querySchemaVersionMetadataResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    querySchemaVersionMetadataRequest,
                    glueClient::querySchemaVersionMetadata
                );

        } catch (final AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        validateMetadataPresence(
            querySchemaVersionMetadataResponse,
            metadataKey,
            metadataValue,
            identifier
        );

        logger.log(
            String.format("%s for %s has successfully been read.",
                ResourceModel.TYPE_NAME,
                schemaVersionId
            ));
        return querySchemaVersionMetadataResponse;
    }

    private ResourceModel toResourceModel(
        final QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse) {
        final ArrayList<Map.Entry<String, MetadataInfo>> metadataInfo =
            Lists.newArrayList(querySchemaVersionMetadataResponse.metadataInfoMap().entrySet());

        final String metadataKey = metadataInfo.get(ONLY_ELEMENT).getKey();
        final String metadataValue = metadataInfo.get(ONLY_ELEMENT).getValue().metadataValue();

        return
            ResourceModel
                .builder()
                .schemaVersionId(querySchemaVersionMetadataResponse.schemaVersionId())
                .key(metadataKey)
                .value(metadataValue)
                .build();
    }

    private void validateMetadataPresence(
        final QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse,
        final String key,
        final String value,
        final String identifier
    ) {
        if (!querySchemaVersionMetadataResponse.hasMetadataInfoMap()
            || !querySchemaVersionMetadataResponse.metadataInfoMap().containsKey(key)
            || !value.equals(querySchemaVersionMetadataResponse.metadataInfoMap().get(key).metadataValue())) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, identifier);
        }
    }

    private QuerySchemaVersionMetadataRequest fromResourceModel(
        final ResourceModel resourceModel) {
        return
            QuerySchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(resourceModel.getSchemaVersionId())
                .metadataList(
                    MetadataKeyValuePair
                        .builder()
                        .metadataKey(resourceModel.getKey())
                        .metadataValue(resourceModel.getValue())
                        .build()
                )
                .build();
    }
}
