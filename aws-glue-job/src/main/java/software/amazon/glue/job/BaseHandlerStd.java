package software.amazon.glue.job;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnThrottlingException;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import com.google.common.collect.ImmutableSet;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import java.util.Set;
import software.amazon.cloudformation.proxy.OperationStatus;
import static java.util.Objects.requireNonNull;

	 public abstract class BaseHandlerStd  extends BaseHandler<CallbackContext>{
	     private final GlueClient glueClient;

	     protected BaseHandlerStd() {
	         this(ClientBuilder.getClient());
	     }

		protected BaseHandlerStd(GlueClient glueClient) {
			this.glueClient = requireNonNull(glueClient);
	}

	     private GlueClient getGlueClient() {
	         return glueClient;
	     }

	     public static final int GENERATED_PHYSICAL_ID_MAX_LEN = 40;
		 static final int CALLBACK_DELAY = 1;
	     public static final String ENTITY_NOT_FOUND_EXCEPTION = "EntityNotFoundException";
	     public static final String INVALID_INPUT_EXCEPTION = "InvalidInputException";
	     public static final String INTERNAL_SERVICE_EXCEPTION = "InternalServiceException";
	     public static final String OPERATION_TIMEOUT_EXCEPTION = "OperationTimeoutException";
	     public static final String NAME_CANNOT_BE_EMPTY = "Model validation failed. Required key [Name] cannot be empty.";
	     public static final String NO_NAME_OR_ROLE_OR_COMMAND_ERROR_MESSAGE = "Model validation failed. Required fields name, role and command cannot be empty.";
	     public static final String IDEMPOTENT_PARAMETER_MISMATCH_EXCEPTION = "IdempotentParameterMismatchException";
	     public static final String RESOURCE_NUMBER_LIMIT_EXCEEDED_EXCEPTION = "ResourceNumberLimitExceededException";
	     public static final String CONCURRENT_MODIFICATION_EXCEPTION = "ConcurrentModificationException";
	     public static final String ALREADY_EXISTS_EXCEPTION = "AlreadyExistsException";
	     public static final String ALREADY_EXISTS = "AlreadyExists";
	     public static final String NOT_AUTHORIZED_EXCEPTION = "NotAuthorizedException";
	     public static final String INVALID_REQUEST = "InvalidRequest";
	     public static final String NOT_FOUND = "NotFound";
	     public static final String REQUEST_LIMIT_EXCEEDED = "RequestLimitExceeded";
	     public static final String SERVICE_INTERNAL_ERROR = "ServiceInternalError";
		static final String ACCESS_DENIED_EXCEPTION = "AccessDeniedException";
		static final String ACCESS_DENIED = "AccessDenied";
		public static final String THROTTLING_EXCEPTION = "ThrottlingException";
  		public static final String THROTTLING_ERROR_CODE = "Throttling";
  		public static final String TOO_MANY_REQUESTS_EXCEPTION = "TooManyRequestsException";
		protected static final Set<String> TAGGING_PERMISSION_ERRORS = ImmutableSet.of(
								 "is not authorized to perform: glue:TagResource",
								 "is not authorized to perform: glue:UntagResource");

	     @Override
	     public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
	             final AmazonWebServicesClientProxy proxy,
	             final ResourceHandlerRequest<ResourceModel> request,
	             final CallbackContext callbackContext,
	             final Logger logger) {
	         return handleRequest(
	                 proxy,
	                 request,
	                 callbackContext != null ? callbackContext : new CallbackContext(),
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
	      * @param proxyClient
	      * @param resourceModel
	      * @param callbackContext
	      * @return
	      */
	     protected ProgressEvent<ResourceModel, CallbackContext> handleError(
	             final GlueRequest request,
	             final Logger logger,
	             final Exception e,
	             final ProxyClient<GlueClient> proxyClient,
	             final ResourceModel resourceModel,
	             final CallbackContext callbackContext) {

				String errorMessage = getErrorCode(e);

				logger.log(String.format("[ERROR] Failed Request: %s, Error Message: %s", request, errorMessage));

	         BaseHandlerException bhe;

			if (ENTITY_NOT_FOUND_EXCEPTION.equals(errorMessage) || NOT_FOUND.equals(errorMessage )) {
				bhe = new CfnNotFoundException(e);
			} else if (ACCESS_DENIED_EXCEPTION.equals(errorMessage) || NOT_AUTHORIZED_EXCEPTION.equals(errorMessage)) {
				bhe  = new CfnAccessDeniedException(e);
			} else if (INVALID_INPUT_EXCEPTION.equals(errorMessage)) {
				bhe = new CfnInvalidRequestException(e);
			} else if (INTERNAL_SERVICE_EXCEPTION.equals(errorMessage) || SERVICE_INTERNAL_ERROR.equals(errorMessage)) {
				bhe = new CfnServiceInternalErrorException(e);
			} else if (OPERATION_TIMEOUT_EXCEPTION.equals(errorMessage)) {
				bhe = new CfnThrottlingException(e);
				logger.log(String.format("Error during operation: %s, Error message: %s", this.getClass().getSimpleName(), e.getMessage()));
				return ProgressEvent.failed(resourceModel, callbackContext, bhe.getErrorCode(), bhe.getMessage());
			} else if (IDEMPOTENT_PARAMETER_MISMATCH_EXCEPTION.equals(errorMessage)) {
				bhe = new CfnAlreadyExistsException(e);
			} else if (RESOURCE_NUMBER_LIMIT_EXCEEDED_EXCEPTION.equals(errorMessage)) {
				bhe = new CfnServiceLimitExceededException(e);
			} else if (CONCURRENT_MODIFICATION_EXCEPTION.equals(errorMessage)) {
				bhe = new CfnNotUpdatableException(e);
			} else if (ALREADY_EXISTS_EXCEPTION.equals(errorMessage) || ALREADY_EXISTS.equals(errorMessage)) {
				bhe = new CfnAlreadyExistsException(e);
				return ProgressEvent.failed(null, null, bhe.getErrorCode(), bhe.getMessage());
			} 	else {
				bhe = new CfnGeneralServiceException(e);
			}

			if (e instanceof AwsServiceException) {

				AwsErrorDetails error = ((AwsServiceException) e).awsErrorDetails();
				final int errorStatus = ((AwsServiceException) e).statusCode();
  			    final String errorCode = error != null ? error.errorCode() : "";
				if (error != null && error.errorCode() != null && error.errorMessage() != null &&
				TAGGING_PERMISSION_ERRORS.stream().anyMatch(error.errorMessage()::contains))
				{

					return ProgressEvent.failed(resourceModel, callbackContext, HandlerErrorCode.UnauthorizedTaggingOperation,
							e.getMessage());
				}
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

			return ProgressEvent.failed(resourceModel, callbackContext, bhe.getErrorCode(), bhe.getMessage());
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
