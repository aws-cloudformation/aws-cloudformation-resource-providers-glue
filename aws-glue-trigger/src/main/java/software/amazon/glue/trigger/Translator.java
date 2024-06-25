package software.amazon.glue.trigger;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.glue.model.CreateTriggerRequest;
import software.amazon.awssdk.services.glue.model.CreateTriggerResponse;
import software.amazon.awssdk.services.glue.model.DeleteTriggerRequest;
import software.amazon.awssdk.services.glue.model.GetTagsRequest;
import software.amazon.awssdk.services.glue.model.GetTriggerRequest;
import software.amazon.awssdk.services.glue.model.ListTriggersRequest;
import software.amazon.awssdk.services.glue.model.ListTriggersResponse;
import software.amazon.awssdk.services.glue.model.TagResourceRequest;
import software.amazon.awssdk.services.glue.model.Trigger;
import software.amazon.awssdk.services.glue.model.TriggerUpdate;
import software.amazon.awssdk.services.glue.model.UntagResourceRequest;
import software.amazon.awssdk.services.glue.model.UpdateTriggerRequest;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Collection;
import java.util.stream.Stream;
import java.lang.reflect.Array;

public class Translator {

    /**
     * Request to create a resource
     * @param model resource model
     * @param triggerName Trigger name to read
     * @param tags map of tags
     * @return awsRequest the aws service request to read tags
     */
    static CreateTriggerRequest translateToCreateRequest(
            final Map<String, String> tags,
            final String triggerName,
            final ResourceModel model){

        if(CollectionUtils.isNullOrEmpty(tags)) {
            return CreateTriggerRequest.builder()
                    .name(triggerName)
                    .type(model.getType())
                    .workflowName(model.getWorkflowName())
                    .schedule(model.getSchedule())
                    .description(model.getDescription())
                    .startOnCreation(model.getStartOnCreation())
                    .eventBatchingCondition(translateToSDKEventBatchingCondition(model.getEventBatchingCondition()))
                    .actions(translateToSDKActions(model.getActions()))
                    .predicate(translateToSDKPredicate(model.getPredicate()))
                    .tags(Collections.emptyMap())
                    .build();
        }
        return CreateTriggerRequest.builder()
                .name(triggerName)
                .type(model.getType())
                .workflowName(model.getWorkflowName())
                .schedule(model.getSchedule())
                .description(model.getDescription())
                .startOnCreation(model.getStartOnCreation())
                .eventBatchingCondition(translateToSDKEventBatchingCondition(model.getEventBatchingCondition()))
                .actions(translateToSDKActions(model.getActions()))
                .predicate(translateToSDKPredicate(model.getPredicate()))
                .tags(tags)
                .build();
    }

    /**
     * Request to list resources
     * @param nextToken nextToken
     * @return awsRequest the aws service request to list resources
     */
    static ListTriggersRequest translateToListRequest(final String nextToken) {
        return ListTriggersRequest.builder()
                .nextToken(nextToken)
                .build();
    }

    /**
     * Request to delete a resource
     * @param model resource model
     * @return awsRequest the aws service request to delete a resource
     */
    static DeleteTriggerRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteTriggerRequest.builder()
                .name(model.getName())
                .build();
    }

    /**
     * Request to update a resource
     * @param desiredModel resource model
     * @return awsRequest the aws service request to update a resource
     */
    static UpdateTriggerRequest translateToUpdateRequest(final ResourceModel desiredModel) {

        return UpdateTriggerRequest.builder()
                .name(desiredModel.getName())
                .triggerUpdate(translateToTriggerUpdate(desiredModel))
                .build();
    }

    /**
     * This method forms the TriggerUpdate
     *
     * @param desiredModel the resource model
     * @return TriggerUpdate
     */
    static TriggerUpdate translateToTriggerUpdate(final ResourceModel desiredModel) {
        final TriggerUpdate.Builder builder = TriggerUpdate.builder()
                .name(desiredModel.getName())
                .actions(translateToSDKActions(desiredModel.getActions()));

        final String description = desiredModel.getDescription();
        final Predicate predicate = desiredModel.getPredicate();
        final EventBatchingCondition eventBatchingCondition = desiredModel.getEventBatchingCondition();
        final String schedule = desiredModel.getSchedule();

        if (!StringUtils.isNullOrEmpty(description)) builder.description(description);
        if (predicate != null) builder.predicate(translateToSDKPredicate(predicate));
        if (eventBatchingCondition != null) builder.eventBatchingCondition(translateToSDKEventBatchingCondition(eventBatchingCondition));
        if (!StringUtils.isNullOrEmpty(schedule)) builder.schedule(schedule);


        return builder.build();
    }

    /**
     * Request to read a resource
     * @param triggerName Trigger name to read
     * @return awsRequest the aws service request to read a resource
     */
    static GetTriggerRequest translateToReadRequest(final String triggerName){
        return GetTriggerRequest.builder()
                .name(triggerName)
                .build();
    }

    /**
     * Request to read tags
     * @param arn Trigger ARN
     * @return awsRequest the aws service request to read tags
     */
    static GetTagsRequest translateToReadTagRequest(final String arn){
        return GetTagsRequest.builder()
                .resourceArn(arn)
                .build();
    }

    /**
     * Request to untag resource
     * @param tagsToDelete tags to be removed
     * @param arn resource ARN
     * @return awsRequest the aws service request to untag resources
     */
    static UntagResourceRequest translateToRemoveTagsRequest(final Map<String, String> tagsToDelete, final String arn) {
        return UntagResourceRequest.builder()
                .resourceArn(arn)
                .tagsToRemove(getTagsToDeleteKeys(tagsToDelete))
                .build();
    }

    /**
     * Request to tag resource
     * @param tagsToCreate tags to be added
     * @param arn resource ARN
     * @return awsRequest the aws service request to tag resources
     */
    static TagResourceRequest translateToCreateTagsRequest(final Map<String, String> tagsToCreate, final String arn) {
        return TagResourceRequest.builder()
                .resourceArn(arn)
                .tagsToAdd(tagsToCreate)
                .build();
    }

    /**
     * Translates resource object from sdk into a resource model
     * @param callbackContext The holder of data already read
     * @return model resource model
     */
    static ResourceModel translateFromReadResponse(final CallbackContext callbackContext){

        Trigger trigger = callbackContext.getTriggerResponse.trigger();
        Map<String, String> tags = callbackContext.getTagsResponse.tags();

        return ResourceModel.builder()
                .type(trigger.typeAsString())
                .description(trigger.description())
                .workflowName(trigger.workflowName())
                .schedule(trigger.schedule())
                .name(trigger.name())
                .actions(translateToModelActions(trigger.actions()))
                .predicate(translateToModelPredicate(trigger.predicate()))
                .eventBatchingCondition(translateToModelEventBatchingCondition(trigger.eventBatchingCondition()))
                .tags(convertStringMapToObjectMap(tags))
                .build();
    }

    /**
     * This method translates list response to list of resource models.
     *
     * @param listTriggersResponse ListTriggerResponse
     * @return List<ResourceModel>
     */
    static List<ResourceModel> translateFromListResponse(final ListTriggersResponse listTriggersResponse) {
        return streamOfOrEmpty(listTriggersResponse.triggerNames())
                .map(name -> ResourceModel.builder()
                        .name(name)
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * This method translates createTrigger response to a resource models.
     *
     * @param createTriggerResponse createTriggerResponse
     * @return ResourceModel
     */
    static ResourceModel translateFromCreateResponse( final CreateTriggerResponse createTriggerResponse) {
        return ResourceModel.builder()
                        .name(createTriggerResponse.name())
                        .build();
    }

    /**
     * This is a Generic method and returns steam of collection if collection is not empty. If Collection is empty and
     * return empty stream.
     *
     * @param <T>
     * @param collection
     * @return Stream<T>
     */
    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    /**
     * This method Translates List of sdk Action into list of model Action
     *
     * @param actions List<software.amazon.awssdk.services.glue.model.Action>
     * @return List<Action>
     */
    static List<Action> translateToModelActions(final List<software.amazon.awssdk.services.glue.model.Action> actions) {

        return streamOfOrEmpty(actions)
                .map(Translator::translateToModelAction)
                .collect(Collectors.toList());
    }

    /**
     * This method Translates software.amazon.awssdk.services.glue.model.Action to Action
     *
     * @param action the action object
     * @return Action
     */
    static Action translateToModelAction(final software.amazon.awssdk.services.glue.model.Action action) {
        if (isEmpty(action)) {
            return null;
        }
        return Action.builder()
                .crawlerName(action.crawlerName())
                .timeout(action.timeout())
                .jobName(action.jobName())
                .arguments(convertStringMapToObjectMap(action.arguments()))
                .securityConfiguration(action.securityConfiguration())
                .notificationProperty(translateToModelNotificationProperty(action.notificationProperty()))
                .build();
    }

    /**
     * This method Translates List of model Action into list of sdk Action
     *
     * @param actions List<Action>
     * @return List<software.amazon.awssdk.services.glue.model.Action>
     */
    static List<software.amazon.awssdk.services.glue.model.Action> translateToSDKActions(final List<Action> actions) {

        return streamOfOrEmpty(actions).map(Translator::translateToSDKAction)
                .collect(Collectors.toList());
    }

    /**
     * This method Translates Action to software.amazon.awssdk.services.glue.model.Action
     *
     * @param action the action object
     * @return software.amazon.awssdk.services.glue.model.Action object
     */
    static software.amazon.awssdk.services.glue.model.Action translateToSDKAction(final Action  action) {
        if (isEmpty(action)) {
            return null;
        }

        return software.amazon.awssdk.services.glue.model.Action.builder()
                .crawlerName(action.getCrawlerName())
                .timeout(action.getTimeout())
                .jobName(action.getJobName())
                .arguments(convertObjectMapToStringMap(getActionArguments(action)))
                .securityConfiguration(action.getSecurityConfiguration())
                .notificationProperty(translateToSDKNotificationProperty(action.getNotificationProperty()))
                .build();
    }

    static Map<String, Object> getActionArguments(Action action) {
        Map<String, Object> arguments = action.getArguments();
        Map<String, Object> argumentsCopy = new HashMap<>();
        if (arguments != null) {
            argumentsCopy.putAll(arguments);
        }
        return argumentsCopy;
    }

    /**
     * This method converts the Map<String, String> into Map<String, Object>
     *
     * @param stringMap Map<String, String>
     * @return Map<String, Object>
     */
    static Map<String, Object> convertStringMapToObjectMap(final Map<String, String> stringMap) {
        if (stringMap == null) {
            return Collections.emptyMap();
        }
        return stringMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2));
    }

    /**
     * This method converts the Map<String, Object> into Map<String, String>
     *
     * @param objectMap Map<String, Object>
     * @return Map<String, String>
     */
    static Map<String, String> convertObjectMapToStringMap(final Map<String, Object> objectMap) {

        if (objectMap == null) {
            return Collections.emptyMap();
        }

        return objectMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
    }

    /**
     * This method Translates software.amazon.awssdk.services.glue.model.NotificationProperty to NotificationProperty
     *
     * @param notificationProperty the notificationProperty object
     * @return NotificationProperty
     */
    static NotificationProperty translateToModelNotificationProperty(
            final software.amazon.awssdk.services.glue.model.NotificationProperty notificationProperty) {

        if (isEmpty(notificationProperty)) {
            return null;
        }
        return NotificationProperty.builder()
                .notifyDelayAfter(notificationProperty.notifyDelayAfter())
                .build();
    }

    /**
     * This method Translates NotificationProperty to software.amazon.awssdk.services.glue.model.NotificationProperty
     *
     * @param notificationProperty the NotificationProperty object
     * @return software.amazon.awssdk.services.glue.model.NotificationProperty object
     */
    static software.amazon.awssdk.services.glue.model.NotificationProperty translateToSDKNotificationProperty(
            final NotificationProperty notificationProperty) {

        if (isEmpty(notificationProperty)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.NotificationProperty.builder()
                .notifyDelayAfter(notificationProperty.getNotifyDelayAfter())
                .build();
    }

    /**
     * This method Translates software.amazon.awssdk.services.glue.model.Predicate to Predicate
     *
     * @param predicate the predicate object
     * @return Predicate
     */
    static Predicate translateToModelPredicate(final software.amazon.awssdk.services.glue.model.Predicate predicate) {
        if (isEmpty(predicate)) {
            return null;
        }
        return Predicate.builder()
                .logical(predicate.logicalAsString())
                .conditions(translateToModelConditions(predicate.conditions()))
                .build();
    }

    /**
     * This method Translates Predicate to software.amazon.awssdk.services.glue.model.Predicate
     *
     * @param predicate the predicate object
     * @return software.amazon.awssdk.services.glue.model.Predicate object
     */
    static software.amazon.awssdk.services.glue.model.Predicate translateToSDKPredicate(final Predicate predicate) {

        if (isEmpty(predicate)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.Predicate.builder()
                .logical(predicate.getLogical())
                .conditions(translateToSDKConditions(predicate.getConditions()))
                .build();
    }

    /**
     * This method Translates List of sdk condition into list of model condition
     *
     * @param conditions List<software.amazon.awssdk.services.glue.model.Condition>
     * @return List<Condition>
     */
    static List<Condition> translateToModelConditions(
            final List<software.amazon.awssdk.services.glue.model.Condition> conditions) {

        return streamOfOrEmpty(conditions).map(Translator::translateToModelCondition)
                .collect(Collectors.toList());
    }

    /**
     * This method Translates software.amazon.awssdk.services.glue.model.Condition to Condition
     *
     * @param condition the software.amazon.awssdk.services.glue.model.Condition object
     * @return Condition object
     */
    static Condition translateToModelCondition(final software.amazon.awssdk.services.glue.model.Condition condition) {

        if (isEmpty(condition)) {
            return null;
        }
        return Condition.builder()
                .logicalOperator(condition.logicalOperatorAsString())
                .jobName(condition.jobName())
                .state(condition.stateAsString())
                .crawlerName(condition.crawlerName())
                .crawlState(condition.crawlStateAsString())
                .build();
    }

    /**
     * This method Translates List of model condition into list of sdk condition
     *
     * @param conditions List<Condition>
     * @return List<software.amazon.awssdk.services.glue.model.Condition>
     */
    static List<software.amazon.awssdk.services.glue.model.Condition> translateToSDKConditions(
            final List<Condition> conditions) {

        return streamOfOrEmpty(conditions).map(Translator::translateToSDKCondition)
                .collect(Collectors.toList());
    }

    /**
     * This method Translates Condition to software.amazon.awssdk.services.glue.model.Condition
     *
     * @param condition the Condition object
     * @return software.amazon.awssdk.services.glue.model.Condition object
     */
    static software.amazon.awssdk.services.glue.model.Condition translateToSDKCondition(final Condition condition) {

        if (isEmpty(condition)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.Condition.builder()
                .logicalOperator(condition.getLogicalOperator())
                .jobName(condition.getJobName())
                .state(condition.getState())
                .crawlerName(condition.getCrawlerName())
                .crawlState(condition.getCrawlState())
                .build();
    }

    /**
     * This method Translates software.amazon.awssdk.services.glue.model.EventBatchingCondition to EventBatchingCondition
     *
     * @param eventBatchingCondition the software.amazon.awssdk.services.glue.model.EventBatchingCondition object
     * @return EventBatchingCondition object
     */
    static EventBatchingCondition translateToModelEventBatchingCondition(
            final software.amazon.awssdk.services.glue.model.EventBatchingCondition eventBatchingCondition) {

        if (isEmpty(eventBatchingCondition)) {
            return null;
        }
        return EventBatchingCondition.builder()
                .batchSize(eventBatchingCondition.batchSize())
                .batchWindow(eventBatchingCondition.batchWindow())
                .build();
    }

    /**
     * This method Translates EventBatchingCondition to software.amazon.awssdk.services.glue.model.EventBatchingCondition
     *
     * @param eventBatchingCondition the eventBatchingCondition object
     * @return software.amazon.awssdk.services.glue.model.EventBatchingCondition object
     */
    static software.amazon.awssdk.services.glue.model.EventBatchingCondition
    translateToSDKEventBatchingCondition(final EventBatchingCondition eventBatchingCondition) {

        if (isEmpty(eventBatchingCondition)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.EventBatchingCondition.builder()
                .batchSize(eventBatchingCondition.getBatchSize())
                .batchWindow(eventBatchingCondition.getBatchWindow())
                .build();
    }

    /**
     * This method extracts the Tag keys and add them to a list
     *
     * @param tagList Map of tags
     * @return list of tag keys
     */
    static List<String> getTagsToDeleteKeys(final Map<String, String> tagList) {
        final List<String> tagListConverted = new ArrayList<>();
        if (tagList != null) {
            tagList.forEach((key, value) -> tagListConverted.add(key));
        }
        return tagListConverted;
    }

    private static boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        } else if (object instanceof CharSequence) {
            return ((CharSequence)object).length() == 0;
        } else if (object.getClass().isArray()) {
            return Array.getLength(object) == 0;
        } else if (object instanceof Collection) {
            return ((Collection)object).isEmpty();
        } else {
            return object instanceof Map ? ((Map)object).isEmpty() : false;
        }
    }
}
