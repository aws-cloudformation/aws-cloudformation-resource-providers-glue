package software.amazon.glue.job;

import software.amazon.awssdk.services.glue.GlueClient;
import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.glue.model.DeleteJobRequest;
import software.amazon.awssdk.services.glue.model.DeleteJobResponse;
import software.amazon.awssdk.services.glue.model.GetJobRequest;
import software.amazon.awssdk.services.glue.model.GetJobResponse;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {

    private Logger logger;

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger) {

        this.logger = logger;

        final ResourceModel model = request.getDesiredResourceState();

        if(model == null || StringUtils.isNullOrEmpty(model.getName())){
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest, NAME_CANNOT_BE_EMPTY);
        }

		return ProgressEvent.progress(model, callbackContext)
		.checkExistence(request, progress -> checkExistence(proxy, request, callbackContext, proxyClient, logger, model))
	    .then(progress -> deleteJob(proxyClient, callbackContext, model, logger, proxy))
		.then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteJob(
            final ProxyClient<GlueClient> proxyClient,
            final CallbackContext callbackContext,
            final ResourceModel model,
            final Logger logger,
            final AmazonWebServicesClientProxy proxy){

        return proxy.initiate("AWS-Glue-Job::DeleteHandler", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToDeleteRequest)
				.makeServiceCall((awsRequest, client) -> client.injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::deleteJob))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .progress();
            }



private ProgressEvent<ResourceModel, CallbackContext> checkExistence(
	             final AmazonWebServicesClientProxy proxy,
	             final ResourceHandlerRequest<ResourceModel> request,
	             final CallbackContext callbackContext,
	             final ProxyClient<GlueClient> proxyClient,
	             final Logger logger,
	             final ResourceModel model
	     ) {
	         if (callbackContext.isDeletePreExistenceCheckDone()) {
	             return ProgressEvent.progress(model, callbackContext);
	         }

	         logger.log(String.format("[ClientRequestToken: %s][StackId: %s] Entered Delete Handler (existence check)",
	                 request.getClientRequestToken(), request.getStackId()));
	         return proxy.initiate("AWS-Glue-Job::DeleteCheckExistence", proxyClient,
	                         model, callbackContext)
	                 .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(resourceModel.getName()))
	                 .makeServiceCall((getRequest, client) -> client.injectCredentialsAndInvokeV2(getRequest,
	                         client.client()::getJob))
	                 .handleError((errorRequest, exception, client, resourceModel, context) ->
	                         handlePreExistenceCheckErrors(errorRequest, exception, proxyClient, resourceModel, context, request))
	                 .done(awsResponse -> {
	                     logger.log(String.format("[ClientRequestToken: %s] Resource exists. Returning control to " +
	                                     "Workflows to continue DELETE (existence check).",
	                             request.getClientRequestToken()));
	                     return ProgressEvent.progress(model, callbackContext);
	                 });
	     }

	     private ProgressEvent<ResourceModel, CallbackContext> handlePreExistenceCheckErrors(
	             final GlueRequest glueRequest,
	             final Exception exception,
	             final ProxyClient<GlueClient> proxyClient,
	             final ResourceModel resourceModel,
	             final CallbackContext callbackContext,
	             final ResourceHandlerRequest<ResourceModel> request
	     ) {
	         callbackContext.setDeletePreExistenceCheckDone(true);

	         final String errorCode = getErrorCode(exception);
	         if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorCode)) {
	             logger.log(String.format("[ClientRequestToken: %s] Resource does not exists." +
	                             "Failing Delete operation. CallbackContext: %s%n",
	                     request.getClientRequestToken(),
	                     callbackContext));

	             return ProgressEvent.failed(
	                     resourceModel,
	                     callbackContext,
	                     HandlerErrorCode.NotFound,
	                     String.format("Job with Id [ %s ] not found", request.getDesiredResourceState().getName())
	             );
	         }
	         return handleError(glueRequest, logger, exception, proxyClient, resourceModel, callbackContext);
	     }
	 }
