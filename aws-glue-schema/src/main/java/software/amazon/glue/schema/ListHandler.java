package software.amazon.glue.schema;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ListSchemasRequest;
import software.amazon.awssdk.services.glue.model.ListSchemasResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.SchemaListItem;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.glue.schema.ResourceModel;
import software.amazon.glue.schema.Registry;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static software.amazon.glue.schema.ExceptionTranslator.translateToCfnException;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        final ListSchemasRequest listSchemasRequest =
            translateToListRequest(request);

        ListSchemasResponse listSchemasResponse = null;
        try {
            listSchemasResponse =
                proxy.injectCredentialsAndInvokeV2(
                    listSchemasRequest,
                    proxyClient.client()::listSchemas
                );
        } catch (AwsServiceException e) {
            final String identifier = String.valueOf(
                listSchemasRequest.registryId());
            translateToCfnException(e, identifier);
        }

        final String nextToken = listSchemasResponse.nextToken();
        final List<ResourceModel> models = translateFromListResponse(listSchemasResponse);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(models)
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private ListSchemasRequest translateToListRequest(
        final ResourceHandlerRequest<ResourceModel> request) {
        final String nextToken = request.getNextToken();

        RegistryId registryId = null;
        final Registry registry = request.getDesiredResourceState().getRegistry();

        if (registry != null) {
            registryId =
                RegistryId
                    .builder()
                    .registryName(registry.getName())
                    .registryArn(registry.getArn())
                    .build();
        }

        return ListSchemasRequest
            .builder()
            .maxResults(50)
            .nextToken(nextToken)
            .registryId(registryId)
            .build();
    }

    private List<ResourceModel> translateFromListResponse(
        final ListSchemasResponse listSchemasResponse) {
        final List<SchemaListItem> schemaListItems = listSchemasResponse.schemas();

        return schemaListItems.stream()
            .map(schemaListItem ->
                ResourceModel
                    .builder()
                    .arn(schemaListItem.schemaArn())
                    .name(schemaListItem.schemaName())
                    .build())
            .collect(toList());
    }
}
