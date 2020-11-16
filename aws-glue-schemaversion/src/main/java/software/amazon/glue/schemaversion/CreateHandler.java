package software.amazon.glue.schemaversion;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionStatus;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static software.amazon.glue.schemaversion.ExceptionTranslator.translateToCfnException;

public class CreateHandler extends BaseHandlerStd {
    private static final Constant BACK_OFF_DELAY =
        Constant
            .of()
            .timeout(Duration.ofSeconds(120L))
            .delay(Duration.ofSeconds(3L))
            .build();

    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<GlueClient> proxyClient,
        final Logger logger) {

        this.logger = logger;
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress -> createSchemaVersion(proxy, proxyClient, progress, "AWS-Glue-SchemaVersion::Create"))
            .then(progress -> stabilize(proxy, proxyClient, progress, "AWS-Glue-SchemaVersion::PostCreateStabilize"))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createSchemaVersion(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<GlueClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(
            callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext()
        )
            .translateToServiceRequest(this::resourceModelToRegisterRequest)
            .backoffDelay(BACK_OFF_DELAY)
            .makeServiceCall(this::registerSchemaVersion)
            //Set VersionId in Resource model for stabilization to use it.
            .done(this::setVersionId);
    }

    private RegisterSchemaVersionResponse registerSchemaVersion(
        final RegisterSchemaVersionRequest registerSchemaVersionRequest,
        final ProxyClient<GlueClient> proxyClient) {

        final GlueClient glueClient = proxyClient.client();
        RegisterSchemaVersionResponse registerSchemaVersionResponse = null;

        final Optional<String> versionId = getSchemaVersionId(
            proxyClient,
            registerSchemaVersionRequest.schemaId(),
            registerSchemaVersionRequest.schemaDefinition()
        );

        //Resource requested to be created already exists.
        if (versionId.isPresent()) {
            throw new CfnAlreadyExistsException(ResourceModel.TYPE_NAME, versionId.get());
        }
        logger.log(
            String.format(
                "SchemaDefinition is not present in schemaId: %s. Proceeding to create",
                registerSchemaVersionRequest.schemaId()
            )
        );

        try {
            registerSchemaVersionResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    registerSchemaVersionRequest,
                    glueClient::registerSchemaVersion
                );
        } catch (final AwsServiceException e) {
            final SchemaId schemaId = registerSchemaVersionRequest.schemaId();
            final String identifier = schemaId == null ? null : schemaId.toString();
            translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "Registered %s with ID %s.",
                ResourceModel.TYPE_NAME,
                registerSchemaVersionResponse.schemaVersionId()
            )
        );
        return registerSchemaVersionResponse;
    }

    /**
     * Even though, we cannot create the same schema version.
     * This check is required to show consistent error messages to customers.
     *
     * @return Optional VersionId if it already exists. Absent otherwise.
     */
    private Optional<String> getSchemaVersionId(
        final ProxyClient<GlueClient> proxyClient,
        final SchemaId schemaId,
        final String schemaDefinition) {
        final GetSchemaByDefinitionRequest getSchemaByDefinitionRequest =
            GetSchemaByDefinitionRequest
                .builder()
                .schemaDefinition(schemaDefinition)
                .schemaId(schemaId)
                .build();
        final GlueClient glueClient = proxyClient.client();

        try {
            GetSchemaByDefinitionResponse response =
                proxyClient
                    .injectCredentialsAndInvokeV2(getSchemaByDefinitionRequest, glueClient::getSchemaByDefinition);
            return Optional.of(response.schemaVersionId());
        } catch (EntityNotFoundException e) {
            return Optional.empty();
        } catch (InvalidInputException e) {
            throw new CfnInvalidRequestException(String.valueOf(schemaId), e);
        } catch (AwsServiceException e) {
            throw
                new CfnGeneralServiceException(
                    String.format("Error determining pre-existence of schema version: %s", e.getMessage())
                );
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> setVersionId(
        final RegisterSchemaVersionRequest registerSchemaVersionRequest,
        final RegisterSchemaVersionResponse registerSchemaVersionResponse,
        final ProxyClient<GlueClient> glueClientProxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext) {

        resourceModel.setVersionId(registerSchemaVersionResponse.schemaVersionId());
        return ProgressEvent.progress(resourceModel, callbackContext);
    }

    private static final BiFunction<ResourceModel, ProxyClient<GlueClient>, ResourceModel> EMPTY_CALL =
        (model, proxyClient) -> model;

    private ProgressEvent<ResourceModel, CallbackContext> stabilize(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<GlueClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(),
            progress.getCallbackContext())
            .translateToServiceRequest(Function.identity())
            .backoffDelay(BACK_OFF_DELAY)
            .makeServiceCall(EMPTY_CALL)
            .stabilize(
                (request, response, proxyInvocation, model, callbackContext) -> isStabilized(proxyClient, response))
            .progress();
    }

    private SchemaVersionStatus getSchemaVersionRegistrationStatus(
        final GetSchemaVersionRequest request,
        final ProxyClient<GlueClient> proxyClient) {

        final GlueClient glueClient = proxyClient.client();

        final GetSchemaVersionResponse getSchemaVersionResponse;
        final SchemaVersionStatus schemaVersionStatus;

        try {
            getSchemaVersionResponse =
                proxyClient.injectCredentialsAndInvokeV2(request, glueClient::getSchemaVersion);

            schemaVersionStatus = getSchemaVersionResponse.status();
        } catch (AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format(
                    "%s creation request accepted but failed to get status due to: %s",
                    ResourceModel.TYPE_NAME,
                    e.getMessage()
                ),
                e);
        }

        logger.log(
            String.format(
                "Creation status of resource %s with ID %s is %s",
                ResourceModel.TYPE_NAME,
                request.schemaVersionId(),
                schemaVersionStatus
            ));

        return schemaVersionStatus;
    }

    private GetSchemaVersionRequest resourceModelToGetRequest(final ResourceModel resourceModel) {
        return GetSchemaVersionRequest
            .builder()
            .schemaVersionId(resourceModel.getVersionId())
            .build();
    }

    private Boolean isStabilized(
        final ProxyClient<GlueClient> proxyClient,
        final ResourceModel resourceModel) {

        final SchemaVersionStatus status =
            getSchemaVersionRegistrationStatus(
                resourceModelToGetRequest(resourceModel),
                proxyClient
            );

        switch (status) {
            case AVAILABLE:
                return true;
            case PENDING:
                return false;
            case FAILURE:
                throw new CfnGeneralServiceException(
                    String.format("Couldn't create %s due to schema evolution failure", ResourceModel.TYPE_NAME));
            case DELETING:
                throw new CfnResourceConflictException(
                    ResourceModel.TYPE_NAME,
                    resourceModel.getVersionId(),
                    String.format("Another process is deleting this %s", ResourceModel.TYPE_NAME));
            default:
                throw new CfnGeneralServiceException(
                    String
                        .format("%s creation request accepted but current status is unknown", ResourceModel.TYPE_NAME));
        }
    }

    private RegisterSchemaVersionRequest resourceModelToRegisterRequest(final ResourceModel resourceModel) {
        final Schema schemaId = resourceModel.getSchema();

        final String schemaArn = schemaId != null ? schemaId.getSchemaArn() : null;
        final String schemaName = schemaId != null ? schemaId.getSchemaName() : null;
        final String registryName = schemaId != null ? schemaId.getRegistryName() : null;

        return
            RegisterSchemaVersionRequest
                .builder()
                .schemaId(
                    SchemaId
                        .builder()
                        .registryName(registryName)
                        .schemaName(schemaName)
                        .schemaArn(schemaArn)
                        .build()
                )
                .schemaDefinition(resourceModel.getSchemaDefinition())
                .build();
    }
}
