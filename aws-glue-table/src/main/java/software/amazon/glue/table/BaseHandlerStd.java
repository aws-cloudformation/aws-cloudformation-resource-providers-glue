package software.amazon.glue.table;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnThrottlingException;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;

import java.lang.Exception;

import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class BaseHandlerStd  extends BaseHandler<CallbackContext>{
    private final GlueClient glueClient;

    static final int CALLBACK_DELAY = 1;
    static final int LIMITED_RETRY_COUNT = 5;

    static final String ENTITY_NOT_FOUND_EXCEPTION = "EntityNotFoundException";
    static final String FEDERATION_SOURCE_EXCEPTION = "FederationSourceException";
    static final String FEDERATION_SOURCE_RETRYABLE_EXCEPTION = "FederationSourceRetryableException";
    static final String GLUE_ENCRYPTION_EXCEPTION = "GlueEncryptionException";
    static final String ALREADY_EXISTS_EXCEPTION = "AlreadyExistsException";
    static final String ALREADY_EXISTS = "AlreadyExists";
    static final String DOES_NOT_EXIST_EXCEPTION = "does not exist";
    //static final String NOT_AUTHORIZED_EXCEPTION = "NotAuthorizedException";
    //static final String NOT_FOUND = "NotFound";
    static final String ACCESS_DENIED_EXCEPTION = "AccessDeniedException";
    static final String ACCESS_DENIED = "AccessDenied";
    static final String INVALID_INPUT_EXCEPTION = "InvalidInputException";
    static final String OPERATION_TIMEOUT_EXCEPTION = "OperationTimeoutException";
    //static final String RESOURCE_NUMBER_LIMIT_EXCEEDED_EXCEPTION = "ResourceNumberLimitExceededException";
    static final String INTERNAL_SERVICE_EXCEPTION = "InternalServiceException";
    static final String RESOURCE_NOT_READY_EXCEPTION = "ResourceNotReadyException";
    //static final String SERVICE_INTERNAL_ERROR = "ServiceInternalError";
    //static final String VERSION_MISMATCH_EXCEPTION = "VersionMismatchException";
    //static final String SCHEDULER_TRANSITIONING_EXCEPTION = "SchedulerTransitioningException";
    static final String REQUEST_LIMIT_EXCEEDED = "RequestLimitExceeded";
    static final String THROTTLING_EXCEPTION = "ThrottlingException";
    static final String THROTTLING_ERROR_CODE = "Throttling";
    static final String TOO_MANY_REQUESTS_EXCEPTION = "TooManyRequestsException";

    protected BaseHandlerStd() {
        this(ClientBuilder.getClient());
    }

    protected BaseHandlerStd(GlueClient glueClient) {
        this.glueClient = requireNonNull(glueClient);
    }

    private GlueClient getGlueClient() {
        return glueClient;
    }

    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return handleRequest(
                proxy,
                request,
                Optional.ofNullable(callbackContext).orElse(new CallbackContext()),
                proxy.newProxy(this::getGlueClient),
                logger
        );
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<GlueClient> proxyClient,
            final Logger logger);

    /**
     * Base Function for handling errors from all the other handlers.
     * @param request
     * @param logger
     * @param e
     * @param resourceModel
     * @param callbackContext
     * @return
     */
    protected ProgressEvent<ResourceModel, CallbackContext> handleError(
            final GlueRequest request,
            final Logger logger,
            final Exception e,
            final ResourceModel resourceModel,
            final CallbackContext callbackContext) {

        String errorMessage = getErrorCode(e);

        logger.log(String.format("[ERROR] Failed Request: %s, Error Message: %s", request, errorMessage));

        BaseHandlerException ex;

        if(e instanceof ConcurrentModificationException && callbackContext.getLimitedRetryCount() < LIMITED_RETRY_COUNT) {
            callbackContext.setLimitedRetryCount(callbackContext.getLimitedRetryCount()+1);
            return ProgressEvent.defaultInProgressHandler(callbackContext, CALLBACK_DELAY, resourceModel);
        }
        if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorMessage)) {
            ex = new CfnNotFoundException(e);
        } else if (ACCESS_DENIED_EXCEPTION.equals(errorMessage)) {
            ex  = new CfnAccessDeniedException(e);
        } else if (OPERATION_TIMEOUT_EXCEPTION.equals(errorMessage)) {
            ex = new CfnThrottlingException(e);
        } else if (INVALID_INPUT_EXCEPTION.equals(errorMessage)) {
            ex = new CfnInvalidRequestException(e);
        } else if (ALREADY_EXISTS.equals(errorMessage)) {
            ex = new CfnAlreadyExistsException(e);
        } else if (e.getMessage().contains(DOES_NOT_EXIST_EXCEPTION)){
            ex = new CfnNotFoundException(e);
        }
         else {
            ex = new CfnGeneralServiceException(e);
        }

        if (e instanceof AwsServiceException) {
            final AwsErrorDetails error = ((AwsServiceException) e).awsErrorDetails();
            final int errorStatus = ((AwsServiceException) e).statusCode();
            final String errorCode = error != null ? error.errorCode() : "";


            // Ref: https://w.amazon.com/bin/view/CloudFormation/wiki/Teams/ResourceProviderCoverage/Docs/Uluru/DevelopingHandlers#HHowtohandleThrottling3F
            if (errorStatus >= 400 && errorStatus < 500) {
                if (THROTTLING_EXCEPTION.equals(errorCode) ||
                        THROTTLING_ERROR_CODE.equals(errorCode) ||
                        REQUEST_LIMIT_EXCEEDED.equals(errorCode) ||
                        TOO_MANY_REQUESTS_EXCEPTION.equals(errorCode)) {
                    logger.log("retrying when Deployment Limit is Exceeded");
                    return buildRetryProgressEvent(resourceModel, callbackContext, HandlerErrorCode.Throttling, CALLBACK_DELAY);
                }
            } else if (errorStatus >= 500) {
                return buildRetryProgressEvent(resourceModel, callbackContext, HandlerErrorCode.Throttling, CALLBACK_DELAY);
            }
        }

        return ProgressEvent.failed(resourceModel, callbackContext, ex.getErrorCode(), ex.getMessage());

    }

    protected static String getErrorCode(Exception e) {
        if (e instanceof AwsServiceException) {
            return ((AwsServiceException) e).awsErrorDetails().errorCode();
        }
        return e.getMessage();
    }

    private ProgressEvent<ResourceModel, CallbackContext> buildRetryProgressEvent(final ResourceModel resourceModel,
                                                                                  final CallbackContext callbackContext,
                                                                                  final HandlerErrorCode errorCode,
                                                                                  int callbackDelay) {
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .callbackContext(callbackContext)
                .resourceModel(resourceModel)
                .errorCode(errorCode)
                .status(OperationStatus.IN_PROGRESS)
                .callbackDelaySeconds(callbackDelay)
                .build();
    }
}
