package software.amazon.glue.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ListRegistriesRequest;
import software.amazon.awssdk.services.glue.model.ListRegistriesResponse;
import software.amazon.awssdk.services.glue.model.RegistryListItem;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static software.amazon.glue.registry.ExceptionTranslator.translateToCfnException;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        final ListRegistriesRequest listRegistriesRequest =
            translateToListRequest(request.getNextToken());

        final GlueClient glueClient = proxyClient.client();

        ListRegistriesResponse listRegistriesResponse = null;

        try {
            listRegistriesResponse =
                proxy.injectCredentialsAndInvokeV2(listRegistriesRequest, glueClient::listRegistries);
        } catch (AwsServiceException e) {
            final String identifier = request.getAwsAccountId();
            translateToCfnException(e, identifier);
        }

        final List<ResourceModel> models = translateFromListResponse(listRegistriesResponse);
        String nextToken = listRegistriesResponse.nextToken();

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(models)
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private ListRegistriesRequest translateToListRequest(final String nextToken) {
        return ListRegistriesRequest
            .builder()
            .maxResults(50)
            .nextToken(nextToken)
            .build();
    }

    private List<ResourceModel> translateFromListResponse(
        final ListRegistriesResponse listRegistriesResponse) {
        final List<RegistryListItem> registryListItems = listRegistriesResponse.registries();

        return registryListItems.stream()
            .map(registryItem ->
                ResourceModel
                    .builder()
                    .arn(registryItem.registryArn())
                    .name(registryItem.registryName())
                    .build())
            .collect(toList());
    }
}
