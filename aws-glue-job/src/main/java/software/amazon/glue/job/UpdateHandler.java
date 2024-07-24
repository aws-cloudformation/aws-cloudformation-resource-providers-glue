package software.amazon.glue.job;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import com.google.common.collect.Sets;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {

    private Logger logger;
    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ResourceModel model = request.getDesiredResourceState();
        final ResourceModel previousModel = request.getPreviousResourceState();
        // callbackContext.setPreviousModel(previousModel);

        if(model == null || StringUtils.isEmpty(model.getName())) {
            return ProgressEvent
            .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, NAME_CANNOT_BE_EMPTY);

        }
        logger.log(String.format("[StackId: %s, ClientRequestToken: %s] Update Job", request.getStackId(), request.getClientRequestToken()));

        return ProgressEvent.progress(model, callbackContext)
            .then(progress -> updateJob(proxy, proxyClient, model, callbackContext, request))
            .then(progress -> updateTags(proxy, proxyClient, progress, request, callbackContext, model, previousModel))
            .then(progress -> ProgressEvent.success(model, callbackContext));


    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateJob(
    final AmazonWebServicesClientProxy proxy,
    final ProxyClient<GlueClient> proxyClient,
    final ResourceModel desiredModel,
    final CallbackContext callbackContext,
    final ResourceHandlerRequest<ResourceModel> request){

        return proxy.initiate("AWS-Glue-Job::UpdateHandler", proxyClient, desiredModel, callbackContext)
            .translateToServiceRequest(resourceModel -> Translator.translateToUpdateRequest(desiredModel))
            .makeServiceCall((updateJobRequest, client) -> {
                logger.log(String.format("[StackId: %s] Invoking update Job request", request.getStackId()));
                return proxyClient.injectCredentialsAndInvokeV2(updateJobRequest, client.client()::updateJob);
            })
            .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
            .progress();

    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTags(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<GlueClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ResourceModel resourceModel,
        final ResourceModel previousModel){


            final Map<String, String> previousTags = new HashMap<>();
            Map<String, String> convertedPreviousTags = Translator.convertObjectMapToStringMap(previousModel.getTags());
            previousTags.putAll(Optional.ofNullable(convertedPreviousTags).orElse(Collections.emptyMap()));
            previousTags.putAll(Optional.ofNullable(request.getPreviousResourceTags()).orElse(Collections.emptyMap()));

            final Map<String, String> desiredTags = new HashMap<>();
            Map<String, String> convertedDesiredTags = Translator.convertObjectMapToStringMap(resourceModel.getTags());
            desiredTags.putAll(Optional.ofNullable(convertedDesiredTags).orElse(Collections.emptyMap()));
            desiredTags.putAll(Optional.ofNullable(request.getDesiredResourceTags()).orElse(Collections.emptyMap()));

            Map<String, String> tagsToDelete = getTagsToDelete(previousTags, desiredTags);
            Map<String, String> tagsToCreate = getTagsToCreate(previousTags, desiredTags);

            return progress
                    .then(_progress -> tagsToDelete.isEmpty()
                            ? ProgressEvent.progress(resourceModel, callbackContext)
                            : deleteTags(proxy, proxyClient, resourceModel, callbackContext, request, tagsToDelete))
                    .then(_progress -> tagsToCreate.isEmpty()
                            ? ProgressEvent.progress(resourceModel, callbackContext)
                            : createTags(proxy, proxyClient, resourceModel, callbackContext, request, tagsToCreate));
        }

        protected ProgressEvent<ResourceModel, CallbackContext> createTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<GlueClient> proxyClient,
            final ResourceModel desiredModel,
            final CallbackContext callbackContext,
            final ResourceHandlerRequest<ResourceModel> request,
            final Map<String, String> tagsToCreate){
                return proxy. initiate("AWS-Glue-Job::CreateTags", proxyClient, desiredModel, callbackContext)
                    .translateToServiceRequest(cbRequest -> Translator.translateToCreateTagsRequest(tagsToCreate, generateArn
                    (request, desiredModel)))
                    .makeServiceCall((cbRequest, cbProxyClient) -> cbProxyClient.injectCredentialsAndInvokeV2(cbRequest, cbProxyClient.client()::tagResource))
                    .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                    .progress();
            }

        protected ProgressEvent<ResourceModel, CallbackContext> deleteTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<GlueClient> proxyClient,
            final ResourceModel desiredModel,
            final CallbackContext callbackContext,
            final ResourceHandlerRequest<ResourceModel> request,
            final Map<String, String> tagsToDelete)
            {
                return proxy.initiate("AWS-Glue-Job::DeleteTags", proxyClient, desiredModel, callbackContext)
                    .translateToServiceRequest(cbRequest -> Translator.translateToRemoveTagsRequest(tagsToDelete, generateArn(request, desiredModel)))
                    .makeServiceCall((cbRequest, cbProxyClient) -> cbProxyClient.injectCredentialsAndInvokeV2(cbRequest, cbProxyClient.client()::untagResource))
                    .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                    .progress();

            }

        private static Map<String, String> getTagsToDelete(
            final Map<String, String> oldTags,
            final Map<String, String> newTags) {

            final Map<String, String> tags = new HashMap<>();
            if (oldTags != null && newTags != null){
                if (newTags.isEmpty())
                {
                    return oldTags;
                } else if (oldTags.isEmpty())
                {
                    return newTags;
                }
                final Set<String> removedKeys = Sets.difference(oldTags.keySet(), newTags.keySet());
                for (String key : removedKeys){
                    if (oldTags.get(key) != null){
                        tags.put(key, oldTags.get(key));
                    }
                }
            }
            return tags;
        }

        private static Map<String, String> getTagsToCreate(
            final Map<String, String> oldTags,
            final Map<String, String> newTags ){

            final Map<String, String> tags = new HashMap<>();
            if (oldTags != null && newTags != null) {
                if (newTags.isEmpty())
                {
                    return Collections.emptyMap();
                }
                else if (oldTags.isEmpty())
                {
                    return newTags;
                }
                final Set<Map.Entry<String, String>> entriesToCreate = Sets.difference(newTags.entrySet(), oldTags.entrySet());
                for (Map.Entry<String, String> entry: entriesToCreate)
                {
                    if(entry.getKey() != null && entry.getValue() != null)
                    {
                        tags.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            return tags;
        }

        private String generateArn(final ResourceHandlerRequest<ResourceModel> request,
                               final ResourceModel model) {

        String regionName = request.getRegion();
        String partition = getPartition(regionName);
        return String.format("arn:%s:glue:%s:%s:job/%s",
                partition,
                regionName,
                request.getAwsAccountId(),
                model.getName());
    }

        private String getPartition(String regionName) {
            if (regionName.matches(".*cn.*")) {
                return "aws-cn";
            } else if (regionName.matches(".*gov.*")) {
                return "aws-us-gov";
            }
            return "aws";
        }

}
