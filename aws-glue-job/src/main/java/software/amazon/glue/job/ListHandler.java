package software.amazon.glue.job;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ListJobsRequest;
import software.amazon.awssdk.services.glue.model.ListJobsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import java.util.ArrayList;
import java.util.List;

public class ListHandler extends BaseHandlerStd {



    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger) {


        final ResourceModel model = request.getDesiredResourceState();
        logger.log(String.format("[StackId: %s, ClientRequestToken: %s] Calling List Jobs", request.getStackId(),
                             request.getClientRequestToken()));

        return listJobs(proxyClient, callbackContext, model, logger, request, proxy);

    }

    private ProgressEvent<ResourceModel, CallbackContext> listJobs(
        final ProxyClient<GlueClient> proxyClient,
        final CallbackContext callbackContext,
        final ResourceModel model,
        final Logger logger,
        final ResourceHandlerRequest<ResourceModel> request,
        final AmazonWebServicesClientProxy proxy
    ){
        return proxy.initiate("AWS-Glue-Job::ListHandler", proxyClient, model, callbackContext)
            .translateToServiceRequest(listRequest -> Translator.translateToListRequest(request.getNextToken()))
            .makeServiceCall((awsRequest, client) -> listJobResponse(proxyClient, awsRequest, logger))
            .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
            .done(listJobsResponse -> ProgressEvent.<ResourceModel, CallbackContext> builder()
                    .resourceModels(Translator.translateFromListResponse(listJobsResponse))
                    .status(OperationStatus.SUCCESS)
                    .nextToken(listJobsResponse.nextToken())
                    .build());
    }

    private ListJobsResponse listJobResponse (
        final ProxyClient<GlueClient> proxyClient,
        final ListJobsRequest awsRequest,
        final Logger logger){
        ListJobsResponse response = proxyClient
                .injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::listJobs);
        logger.log("Successfully listed all jobs.");
        return response;
    }
}
