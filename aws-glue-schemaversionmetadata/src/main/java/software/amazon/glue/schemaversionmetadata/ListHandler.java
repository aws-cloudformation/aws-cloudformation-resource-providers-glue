package software.amazon.glue.schemaversionmetadata;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static software.amazon.glue.schemaversionmetadata.ExceptionTranslator.translateToCfnException;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        final QuerySchemaVersionMetadataRequest querySchemaVersionMetadataRequest =
            translateToListRequest(request);

        QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse = null;

        final String identifier = querySchemaVersionMetadataRequest.schemaVersionId();

        try {
            querySchemaVersionMetadataResponse =
                proxy.injectCredentialsAndInvokeV2(
                    querySchemaVersionMetadataRequest,
                    proxyClient.client()::querySchemaVersionMetadata
                );
        } catch (AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        final String nextToken = querySchemaVersionMetadataResponse.nextToken();
        final List<ResourceModel> models = translateFromListResponse(querySchemaVersionMetadataResponse);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(models)
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private List<ResourceModel> translateFromListResponse(
        final QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse) {

        if (!querySchemaVersionMetadataResponse.hasMetadataInfoMap()) {
            return Collections.emptyList();
        }

        final Map<String, MetadataInfo> metadataInfoMap =
            querySchemaVersionMetadataResponse.metadataInfoMap();

        return
            metadataInfoMap
                .entrySet()
                .stream()
                .map(entry ->
                    ResourceModel
                        .builder()
                        .schemaVersionId(querySchemaVersionMetadataResponse.schemaVersionId())
                        .key(entry.getKey())
                        .value(entry.getValue().metadataValue())
                        .build()
                )
                .collect(toList());
    }

    private QuerySchemaVersionMetadataRequest translateToListRequest(
        final ResourceHandlerRequest<ResourceModel> request) {
        final String schemaVersionId = request.getDesiredResourceState().getSchemaVersionId();

        return QuerySchemaVersionMetadataRequest
            .builder()
            .nextToken(request.getNextToken())
            .schemaVersionId(schemaVersionId)
            .maxResults(50)
            .build();
    }
}
