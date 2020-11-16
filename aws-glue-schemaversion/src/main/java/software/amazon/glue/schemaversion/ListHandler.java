package software.amazon.glue.schemaversion;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsRequest;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsResponse;
import software.amazon.awssdk.services.glue.model.SchemaVersionListItem;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static software.amazon.glue.schemaversion.ExceptionTranslator.translateToCfnException;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        final ListSchemaVersionsRequest listSchemaVersionsRequest =
            translateToListRequest(request);

        ListSchemaVersionsResponse listSchemaVersionsResponse = null;

        final String identifier =
            listSchemaVersionsRequest.schemaId() == null ?
                null : listSchemaVersionsRequest.schemaId().toString();

        try {
            listSchemaVersionsResponse =
                proxy.injectCredentialsAndInvokeV2(
                    listSchemaVersionsRequest,
                    proxyClient.client()::listSchemaVersions
                );
        } catch (AwsServiceException e) {
            translateToCfnException(e, identifier);
        }

        final String nextToken = listSchemaVersionsResponse.nextToken();
        final List<ResourceModel> models = translateFromListResponse(listSchemaVersionsResponse);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(models)
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private List<ResourceModel> translateFromListResponse(
        final ListSchemaVersionsResponse listSchemaVersionsResponse) {
        final List<SchemaVersionListItem> schemas = listSchemaVersionsResponse.schemas();

        return schemas.stream()
            .map(schema ->
                ResourceModel
                    .builder()
                    .versionId(schema.schemaVersionId())
                    .build()
            )
            .collect(toList());
    }

    private ListSchemaVersionsRequest translateToListRequest(
        final ResourceHandlerRequest<ResourceModel> request) {
        final Schema schemaId = request.getDesiredResourceState().getSchema();
        final String nextToken = request.getNextToken();

        software.amazon.awssdk.services.glue.model.SchemaId requestSchemaId = null;

        if (schemaId != null) {
            requestSchemaId =
                software.amazon.awssdk.services.glue.model.SchemaId
                    .builder()
                    .schemaName(schemaId.getSchemaName())
                    .registryName(schemaId.getRegistryName())
                    .schemaArn(schemaId.getSchemaArn())
                    .build();
        }

        return ListSchemaVersionsRequest
            .builder()
            .maxResults(50)
            .nextToken(nextToken)
            .schemaId(requestSchemaId)
            .build();
    }
}
