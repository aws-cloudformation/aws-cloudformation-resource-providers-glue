package software.amazon.glue.job;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Collection;
import java.util.stream.Stream;
import com.google.common.base.Objects;
import com.amazonaws.util.NumberUtils;
import com.amazonaws.util.StringUtils;

import org.apache.commons.collections.MapUtils;

import org.json.JSONObject;

import software.amazon.awssdk.services.glue.model.CreateJobRequest;
import software.amazon.awssdk.services.glue.model.CreateJobResponse;
import software.amazon.awssdk.services.glue.model.DeleteJobRequest;
import software.amazon.awssdk.services.glue.model.GetTagsRequest;
import software.amazon.awssdk.services.glue.model.GetJobRequest;
import software.amazon.awssdk.services.glue.model.ListJobsRequest;
import software.amazon.awssdk.services.glue.model.ListJobsResponse;
import software.amazon.awssdk.services.glue.model.TagResourceRequest;
import software.amazon.awssdk.services.glue.model.Job;
import software.amazon.awssdk.services.glue.model.JobUpdate;
import software.amazon.awssdk.services.glue.model.UntagResourceRequest;;
import software.amazon.awssdk.services.glue.model.UpdateJobRequest;
import software.amazon.awssdk.utils.CollectionUtils;
public class Translator {

    static GetJobRequest translateToReadRequest(final String jobName){

        return GetJobRequest.builder()
            .jobName(jobName)
            .build();
    }

    static GetTagsRequest translateToReadTagRequest(final String arn){
        return GetTagsRequest.builder()
            .resourceArn(arn)
            .build();
    }
    public static UntagResourceRequest translateToRemoveTagsRequest(Map<String, String> tagsToDelete, String arn) {
             return UntagResourceRequest.builder()
                     .resourceArn(arn)
                     .tagsToRemove(getTagsToDeleteKeys(tagsToDelete))
                     .build();
    }

    public static TagResourceRequest translateToCreateTagsRequest(Map<String, String> tagsToCreate, String arn) {
        return TagResourceRequest.builder()
                .resourceArn(arn)
                .tagsToAdd(tagsToCreate)
                .build();
    }

    static CreateJobRequest translateToCreateRequest(
        final Map<String, String> tags,
        final String jobName,
        final ResourceModel model)
        {
        if(CollectionUtils.isNullOrEmpty(tags)){
            return CreateJobRequest.builder()
                .name(jobName)
                .description(model.getDescription())
                .logUri(model.getLogUri())
                .role(model.getRole())
                .executionProperty(translateToSDKCompatibleExecutionProperty(model.getExecutionProperty()))
                .command(translateToSDKCompatibleJobCommand(model.getCommand()))
                .defaultArguments(convertObjectMapToStringMap(model.getDefaultArguments()))
                .nonOverridableArguments(convertObjectMapToStringMap(model.getNonOverridableArguments()))
                .connections(translateToSDKCompatibleConnections(model.getConnections()))
                .maxRetries(doubleToInt(model.getMaxRetries()))
                .allocatedCapacity(doubleToInt(model.getAllocatedCapacity()))
                .timeout(model.getTimeout())
                .maxCapacity(model.getMaxCapacity())
                .securityConfiguration(model.getSecurityConfiguration())
                .tags(Collections.emptyMap())
                .notificationProperty(translateToSDKCompatibleNotificationProperty(model.getNotificationProperty()))
                .glueVersion(model.getGlueVersion())
                .numberOfWorkers(model.getNumberOfWorkers())
                .workerType(model.getWorkerType())
                .executionClass(model.getExecutionClass())
                .maintenanceWindow(model.getMaintenanceWindow())
                .build();
        }
        return CreateJobRequest.builder()
                .name(jobName)
                .description(model.getDescription())
                .logUri(model.getLogUri())
                .role(model.getRole())
                .executionProperty(translateToSDKCompatibleExecutionProperty(model.getExecutionProperty()))
                .command(translateToSDKCompatibleJobCommand(model.getCommand()))
                .defaultArguments(convertObjectMapToStringMap(model.getDefaultArguments()))
                .nonOverridableArguments(convertObjectMapToStringMap(model.getNonOverridableArguments()))
                .connections(translateToSDKCompatibleConnections(model.getConnections()))
                .maxRetries(doubleToInt(model.getMaxRetries()))
                .allocatedCapacity(doubleToInt(model.getAllocatedCapacity()))
                .timeout(model.getTimeout())
                .maxCapacity(model.getMaxCapacity())
                .securityConfiguration(model.getSecurityConfiguration())
                .tags(tags)
                .notificationProperty(translateToSDKCompatibleNotificationProperty(model.getNotificationProperty()))
                .glueVersion(model.getGlueVersion())
                .numberOfWorkers(model.getNumberOfWorkers())
                .workerType(model.getWorkerType())
                .executionClass(model.getExecutionClass())
                .maintenanceWindow(model.getMaintenanceWindow())
                .build();

    }
    static ListJobsRequest translateToListRequest(final String nextToken) {
                 return ListJobsRequest.builder()
                         .nextToken(nextToken)
                         .build();
             }

    static DeleteJobRequest translateToDeleteRequest(final ResourceModel model) {
                 return DeleteJobRequest.builder()
                         .jobName(model.getName())
                         .build();
             }

    static UpdateJobRequest translateToUpdateRequest(final ResourceModel desiredModel) {

                 return UpdateJobRequest.builder()
                         .jobName(desiredModel.getName())
                         .jobUpdate(translateToJobUpdate(desiredModel))
                         .build();
             }



    static JobUpdate translateToJobUpdate(final ResourceModel desiredModel){
        final JobUpdate.Builder builder = JobUpdate.builder()
            .role(desiredModel.getRole())
            .command(translateToSDKCompatibleJobCommand(desiredModel.getCommand()));

        final Number maxCapacity = desiredModel.getMaxCapacity();
        final Number maxRetries = desiredModel.getMaxRetries();

        final Integer numberOfWorkers = desiredModel.getNumberOfWorkers();
        final String securityConfiguration = desiredModel.getSecurityConfiguration();
        final String executionClass = desiredModel.getExecutionClass();
        final String glueVersion = desiredModel.getGlueVersion();
        final String logUri = desiredModel.getLogUri();
        final String workerType = desiredModel.getWorkerType();
        final String description = desiredModel.getDescription();
        final String maintenanceWindow = desiredModel.getMaintenanceWindow();
        final Integer timeout = desiredModel.getTimeout();
        final NotificationProperty notificationProperty = desiredModel.getNotificationProperty();
        final ExecutionProperty executionProperty = desiredModel.getExecutionProperty();
        final ConnectionsList connectionsList = desiredModel.getConnections();
        final Map<String, String> defaultArguments = convertObjectMapToStringMap(desiredModel.getDefaultArguments());
        final Map<String, String> nonOverridableArguments = convertObjectMapToStringMap(desiredModel.getNonOverridableArguments());

        if (notificationProperty != null) builder.notificationProperty(translateToSDKCompatibleNotificationProperty(notificationProperty));
        if (executionProperty != null) builder.executionProperty(translateToSDKCompatibleExecutionProperty(executionProperty));
        if (connectionsList != null) builder.connections(translateToSDKCompatibleConnections(connectionsList));
        if (defaultArguments != null) builder.defaultArguments(defaultArguments);
        if (nonOverridableArguments != null) builder.nonOverridableArguments(nonOverridableArguments);
        if (!StringUtils.isNullOrEmpty(description)) builder.description(description);
        if (!StringUtils.isNullOrEmpty(executionClass)) builder.executionClass(executionClass);
        if (!StringUtils.isNullOrEmpty(glueVersion)) builder.glueVersion(glueVersion);
        if (!StringUtils.isNullOrEmpty(logUri)) builder.logUri(logUri);
        if (!StringUtils.isNullOrEmpty(maintenanceWindow)) builder.maintenanceWindow(maintenanceWindow);
        if (!StringUtils.isNullOrEmpty(securityConfiguration)) builder.securityConfiguration(securityConfiguration);
        if (!StringUtils.isNullOrEmpty(workerType)) builder.workerType(workerType);
        if (maxCapacity != null) builder.maxCapacity(maxCapacity.doubleValue());
        if (numberOfWorkers != null) builder.numberOfWorkers(numberOfWorkers);
        if (timeout != null) builder.timeout(timeout);
        if (maxRetries != null) builder.maxRetries(maxRetries.intValue());





        return builder.build();
    }

    static List<ResourceModel> translateFromListResponse( final ListJobsResponse listJobsResponse) {
        return streamOfOrEmpty(listJobsResponse.jobNames())
                .map(name -> ResourceModel.builder()
                        .name(name)
                        .build())
                .collect(Collectors.toList());
    }

    static ResourceModel translateFromCreateResponse( final CreateJobResponse createJobResponse) {
                 return ResourceModel.builder()
                                 .name(createJobResponse.name())
                                 .build();
             }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    static Map<String, Object> convertStringMapToObjectMap(Map<String, String> stringMap) {
        if (MapUtils.isEmpty(stringMap)) {

            return null;

        }
        return stringMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2));
    }

    static Map<String, String> convertObjectMapToStringMap(Map<String, Object> objectMap) {
        if (MapUtils.isEmpty(objectMap)) {

            return null;
        }
        return objectMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));


    }

    static Integer doubleToInt(Double num){
        if (num == null){
            return null;
        }
        return (int) num.doubleValue();
    }

    static List<String> getTagsToDeleteKeys(final Map<String, String> tagList) {
        final List<String> tagListConverted = new ArrayList<>();
        if (tagList != null) {
            tagList.forEach((key, value) -> tagListConverted.add(key));
        }
        return tagListConverted;
    }

    static ResourceModel translateFromReadResponse(final CallbackContext callbackContext)
    {
        Job job = callbackContext.getJobResponse.job();
        Map<String, String> tags = callbackContext.getTagsResponse.tags();

        return ResourceModel.builder()
                .maxCapacity(job.maxCapacity())
                .numberOfWorkers(job.numberOfWorkers())
                .securityConfiguration(job.securityConfiguration())
                .executionClass(job.executionClassAsString())
                .glueVersion(job.glueVersion())
                .logUri(job.logUri())
                .workerType(job.workerTypeAsString())
                .maxRetries(job.maxRetries() != null ? Double.valueOf(job.maxRetries()) : null)
                .description(job.description())
                .timeout(job.timeout())
                .allocatedCapacity(job.allocatedCapacity() != null ? Double.valueOf(job.allocatedCapacity()) : null)
                .name(job.name())
                .role(job.role())
                .notificationProperty(translateToModelCompatibleNotificationProperty(job.notificationProperty()))
                .command(translateToModelCompatibleJobCommand(job.command()))
                .executionProperty(translateToModelCompatibleExecutionProperty(job.executionProperty()))
                .connections(job.connections() != null ? translateToModelCompatibleConnections( job.connections()): null)
                .defaultArguments(convertStringMapToObjectMap(job.defaultArguments()))
                .nonOverridableArguments(convertStringMapToObjectMap(job.nonOverridableArguments()))
                .tags(convertStringMapToObjectMap(tags))
                .maintenanceWindow(job.maintenanceWindow())
                .build();

    }


   /**
     * This method Translates NotificationProperty to software.amazon.awssdk.services.glue.model.NotificationProperty
    *
    * @param notificationProperty the NotificationProperty object
    * @return software.amazon.awssdk.services.glue.model.NotificationProperty object
    */

    static NotificationProperty translateToModelCompatibleNotificationProperty(
	             final software.amazon.awssdk.services.glue.model.NotificationProperty notificationProperty) {
	         if (Objects.equal(notificationProperty, null)) {
	             return null;
	         }
	         return NotificationProperty.builder()
	                 .notifyDelayAfter(notificationProperty.notifyDelayAfter())
	                 .build();
	     }

    static software.amazon.awssdk.services.glue.model.NotificationProperty translateToSDKCompatibleNotificationProperty(
            final NotificationProperty notificationProperty) {
        if (Objects.equal(notificationProperty,null)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.NotificationProperty.builder()
                .notifyDelayAfter(notificationProperty.getNotifyDelayAfter())
                .build();
        }


    /**
     * This method Translates software.amazon.awssdk.services.glue.model.JobCommand to JobCommand
     *
     * @param jobCommand the jobCommand object
     * @return JobCommand
     */
    static JobCommand translateToModelCompatibleJobCommand(final software.amazon.awssdk.services.glue.model.JobCommand jobCommand) {
        if (Objects.equal(jobCommand, null)) {
            return null;
        }
        return JobCommand.builder()
                .name(jobCommand.name())
                .pythonVersion(jobCommand.pythonVersion())
                .runtime(jobCommand.runtime())
                .scriptLocation(jobCommand.scriptLocation())
                .build();
    }

    static software.amazon.awssdk.services.glue.model.JobCommand translateToSDKCompatibleJobCommand(final JobCommand jobCommand) {
        if (Objects.equal(jobCommand, null)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.JobCommand.builder()
                .name(jobCommand.getName())
                .pythonVersion(jobCommand.getPythonVersion())
                .runtime(jobCommand.getRuntime())
                .scriptLocation(jobCommand.getScriptLocation())
                .build();
    }



    /**
     * This method Translates software.amazon.awssdk.services.glue.model.ExecutionProperty to ExecutionProperty
     *
     * @param executionProperty the executionProperty object
     * @return ExecutionProperty
     */
    static ExecutionProperty translateToModelCompatibleExecutionProperty(final software.amazon.awssdk.services.glue.model.ExecutionProperty executionProperty) {
        if (Objects.equal(executionProperty, null)) {
            return null;
        }
        return ExecutionProperty.builder()
                .maxConcurrentRuns(Double.valueOf(executionProperty.maxConcurrentRuns()))
                .build();
    }

    static software.amazon.awssdk.services.glue.model.ExecutionProperty translateToSDKCompatibleExecutionProperty(final ExecutionProperty executionProperty) {
        if (Objects.equal(executionProperty, null)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.ExecutionProperty.builder()
                .maxConcurrentRuns(doubleToInt(executionProperty.getMaxConcurrentRuns()))
                .build();
    }


    static ConnectionsList translateToModelCompatibleConnections(final software.amazon.awssdk.services.glue.model.ConnectionsList connections) {
        return ConnectionsList.builder()
                .connections(connections.connections())
                .build();
    }

    static software.amazon.awssdk.services.glue.model.ConnectionsList translateToSDKCompatibleConnections(final ConnectionsList connections) {
        if (Objects.equal(connections, null)) {
            return null;
        }
        return software.amazon.awssdk.services.glue.model.ConnectionsList.builder()
                .connections(connections.getConnections())
                .build();
    }


}
