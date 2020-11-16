package software.amazon.glue.schemaversionmetadata;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;

public class ExceptionTranslator {

    private ExceptionTranslator() { }

    public static void translateToCfnException(
        final AwsServiceException exception,
        final String identifier) {
        if (exception instanceof AccessDeniedException) {
            throw new CfnAccessDeniedException(ResourceModel.TYPE_NAME, exception);
        }
        if (exception instanceof AlreadyExistsException) {
            throw new CfnAlreadyExistsException(ResourceModel.TYPE_NAME, identifier, exception);
        }
        if (exception instanceof EntityNotFoundException) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, identifier, exception);
        }
        if (exception instanceof ResourceNumberLimitExceededException) {
            throw new CfnServiceLimitExceededException(ResourceModel.TYPE_NAME, exception.getMessage(), exception);
        }
        if (exception instanceof InvalidInputException) {
            throw new CfnInvalidRequestException(exception);
        }
        throw new CfnGeneralServiceException(exception.getMessage(), exception);
    }

}
