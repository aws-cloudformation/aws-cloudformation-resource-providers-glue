package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import com.google.common.collect.Sets;

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
//        callbackContext.setPreviousModel(previousModel);

        if(model == null || StringUtils.isEmpty(model.getName()) || model.getActions().isEmpty()) {
            return ProgressEvent
                    .failed(model, callbackContext, HandlerErrorCode.InvalidRequest, EMPTY_MODEL_OR_NAME_ACTIONS_ERROR_MESSAGE);
        }

        logger.log(String.format("[StackId: %s, ClientRequestToken: %s] Calling Update Trigger", request.getStackId(), request.getClientRequestToken()));

        return ProgressEvent.progress(model, callbackContext)
                .then(progress -> updateTrigger(proxy, proxyClient, model, callbackContext, request))
                .then(progress -> updateTags(proxy, proxyClient, progress, request, callbackContext, model, previousModel))
                .then(progress -> ProgressEvent.success(model, callbackContext));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> updateTrigger(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<GlueClient> proxyClient,
            final ResourceModel desiredModel,
            final CallbackContext callbackContext,
            final ResourceHandlerRequest<ResourceModel> request) {

        return proxy.initiate("AWS-Glue-Trigger::UpdateHandler", proxyClient, desiredModel, callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.translateToUpdateRequest(desiredModel))
                .makeServiceCall((updateTriggerRequest, client) -> {
                    logger.log(String.format("[StackId: %s] Invoking Update Trigger", request.getStackId()));
                    return proxyClient.injectCredentialsAndInvokeV2(updateTriggerRequest, client.client()::updateTrigger);
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
            final ResourceModel previousModel) {

        // All previous tags
        final Map<String, String> previousTags = new HashMap<>();
        Map<String, String> convertedPreviousTags = Translator.convertObjectMapToStringMap(previousModel.getTags());
        previousTags.putAll(Optional.ofNullable(convertedPreviousTags).orElse(Collections.emptyMap()));
        previousTags.putAll(Optional.ofNullable(request.getPreviousResourceTags()).orElse(Collections.emptyMap()));

        //All desired tags
        final Map<String, String> desiredTags = new HashMap<>();
        Map<String, String> convertedDesiredTags = Translator.convertObjectMapToStringMap(resourceModel.getTags());
        desiredTags.putAll(Optional.ofNullable(convertedDesiredTags).orElse(Collections.emptyMap()));
        desiredTags.putAll(Optional.ofNullable(request.getDesiredResourceTags()).orElse(Collections.emptyMap()));

        // compares previous and current desired tags to determined which tags to be deleted
        Map<String, String> tagsToDelete = getTagsToDelete(previousTags, desiredTags);
        // compares previous and current desired tags to determined which tags to be created
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
            final Map<String, String> tagsToCreate) {

        return proxy.initiate("AWS-Glue-Trigger::CreateTags", proxyClient, desiredModel, callbackContext)
                .translateToServiceRequest(cbRequest -> Translator.translateToCreateTagsRequest(tagsToCreate, generateArn(request, desiredModel)))
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
            final Map<String, String> tagsToDelete) {

        return proxy.initiate("AWS-Glue-Trigger::DeleteTags", proxyClient, desiredModel, callbackContext)
                .translateToServiceRequest(cbRequest -> Translator.translateToRemoveTagsRequest(tagsToDelete, generateArn(request, desiredModel)))
                .makeServiceCall((cbRequest, cbProxyClient) -> cbProxyClient.injectCredentialsAndInvokeV2(cbRequest, cbProxyClient.client()::untagResource))
                .handleError((errorRequest, exception, client, resourceModel, context) -> handleError(errorRequest, logger, exception, client, resourceModel, context))
                .progress();
    }

    private static Map<String, String> getTagsToDelete(
            final Map<String, String> oldTags,
            final Map<String, String> newTags) {

        final Map<String, String> tags = new HashMap<>();
        if (oldTags != null && newTags != null) {
            if (newTags.isEmpty()) {
                return oldTags;
            } else if (oldTags.isEmpty()) {
                return newTags;
            }
            final Set<String> removedKeys = Sets.difference(oldTags.keySet(), newTags.keySet());
            for (String key : removedKeys) {
                if (oldTags.get(key) != null) {
                    tags.put(key, oldTags.get(key));
                }
            }
        }
        return tags;
    }

    private static Map<String, String> getTagsToCreate(
            final Map<String, String> oldTags,
            final Map<String, String> newTags) {

        final Map<String, String> tags = new HashMap<>();
        if (oldTags != null && newTags != null) {
            if (newTags.isEmpty()) {
                return Collections.emptyMap();
            } else if (oldTags.isEmpty()) {
                return newTags;
            }
            final Set<Map.Entry<String, String>> entriesToCreate = Sets.difference(newTags.entrySet(), oldTags.entrySet());
            for (Map.Entry<String, String> entry : entriesToCreate) {
                if (entry.getKey() != null && entry.getValue() != null) {
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
        return String.format("arn:%s:glue:%s:%s:trigger/%s",
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
