package software.amazon.glue.schemaversion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionStatus;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE;
import static software.amazon.awssdk.services.glue.model.SchemaVersionStatus.DELETING;
import static software.amazon.awssdk.services.glue.model.SchemaVersionStatus.FAILURE;
import static software.amazon.awssdk.services.glue.model.SchemaVersionStatus.PENDING;
import static software.amazon.awssdk.services.glue.model.SchemaVersionStatus.UNKNOWN_TO_SDK_VERSION;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    private CreateHandler handler;

    @Mock
    private GlueClient glueClient;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new CreateHandler();
    }

    @Test
    public void handleRequest_WhenRegistrationSucceedsForTransitiveCompatibility_ReturnsSuccess() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(AVAILABLE));

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenReturn(
                TestData.getSchemaVersionResponseWithStatus(AVAILABLE),
                //From ReadHandler.
                TestData.getSchemaVersionResponseWithStatus(AVAILABLE)
            );

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_VERSION_RESPONSE_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenRegistrationSucceedsForNonTransitiveCompatibility_ReturnsSuccess() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(PENDING));

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenReturn(
                //Return pending initially.
                TestData.getSchemaVersionResponseWithStatus(PENDING),
                //Then return available.
                TestData.getSchemaVersionResponseWithStatus(AVAILABLE),
                //Then from Read handler.
                TestData.getSchemaVersionResponseWithStatus(AVAILABLE)
            );

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_VERSION_RESPONSE_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenRegisteredBySchemaName_ReturnsSuccess() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_NAME, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_FOR_VERSION_BY_NAME, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(AVAILABLE));

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenReturn(
                TestData.getSchemaVersionResponseWithStatus(AVAILABLE),
                //Then from Read handler.
                TestData.getSchemaVersionResponseWithStatus(AVAILABLE)
            );

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_NAME,
                new CallbackContext(),
                proxyClient,
                logger
            );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.GET_SCHEMA_VERSION_RESPONSE_RESOURCE_MODEL);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenSchemaEvolutionFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(PENDING));

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenReturn(
                //Return pending initially.
                TestData.getSchemaVersionResponseWithStatus(PENDING),
                //Then return failure.
                TestData.getSchemaVersionResponseWithStatus(FAILURE)
            );

        Exception exception = assertThrows(
            CfnGeneralServiceException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains("Error occurred during operation "
            + "'Couldn't create AWS::Glue::SchemaVersion due to schema evolution failure");
    }

    @Test
    public void handleRequest_WhenSchemaVersionIsDeleted_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(PENDING));

        //Return deleted.
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenReturn(TestData.getSchemaVersionResponseWithStatus(DELETING));

        Exception exception = assertThrows(
            CfnResourceConflictException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains(
            "Resource of type 'AWS::Glue::SchemaVersion' with identifier '" +
                TestData.NEXT_SCHEMA_VERSION_ID + "' has a conflict. "
                + "Reason: Another process is deleting this AWS::Glue::SchemaVersion.");
    }

    @Test
    public void handleRequest_WhenSchemaVersionStatusIsUnknown_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(PENDING));

        //Then return Unknown.
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenReturn(TestData.getSchemaVersionResponseWithStatus(UNKNOWN_TO_SDK_VERSION));

        Exception exception = assertThrows(
            CfnGeneralServiceException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains("Error occurred during operation "
            + "'AWS::Glue::SchemaVersion creation request accepted but current status is unknown'");
    }

    @Test
    public void handleRequest_WhenSchemaVersionStatusLookupFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        //Registration successful.
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenReturn(TestData.getRegisterSchemaVersionResponseWithStatus(PENDING));

        //Fail initial lookup.
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN, glueClient::getSchemaVersion))
            .thenThrow(InternalServiceException.class);

        Exception exception = assertThrows(
            CfnGeneralServiceException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains("Error occurred during operation "
            + "'AWS::Glue::SchemaVersion creation request accepted but failed to get status due to: null");
    }

    @Test
    public void handleRequest_WhenRegistrationRequestFailsWithAccessDenied_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_NO_ID, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_WITH_NO_IDENTIFIER, glueClient::registerSchemaVersion))
            .thenThrow(AccessDeniedException.class);

        Exception exception = assertThrows(
            CfnAccessDeniedException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_REQUEST_WITH_NO_IDENTIFIER,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains("Access denied for operation 'AWS::Glue::SchemaVersion'");
    }

    @Test
    public void handleRequest_WhenVersionAlreadyExists_ThrowsAlreadyExistsException() {
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenReturn(TestData.GET_SCHEMA_BY_DEFINITION_RESPONSE);

        Exception exception = assertThrows(
            CfnAlreadyExistsException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("AWS::Glue::SchemaVersion' with identifier 'yurt9301-dc50-11ea-87d0-8iofb18nkrp8' already exists.");
    }

    @Test
    public void handleRequest_WhenVersionExistsCheckFailsWithInvalidInputException_ThrowsException() {
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(InvalidInputException.class);

        Exception exception = assertThrows(
            CfnInvalidRequestException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Invalid request provided: SchemaId(SchemaArn=arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema)");
    }

    @Test
    public void handleRequest_WhenVersionExistsCheckFailsWithException_ThrowsException() {
        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(InternalServiceException.class);

        Exception exception = assertThrows(
            CfnGeneralServiceException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Error occurred during operation 'Error determining pre-existence of schema version: ");
    }

    @Test
    public void handleRequest_WhenRegistrationRequestFailsWithResourceLimitException_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_NO_ID, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_WITH_NO_IDENTIFIER, glueClient::registerSchemaVersion))
            .thenThrow(ResourceNumberLimitExceededException.class);

        Exception exception = assertThrows(
            CfnServiceLimitExceededException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_REQUEST_WITH_NO_IDENTIFIER,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Limit exceeded for resource of type 'AWS::Glue::SchemaVersion'.");
    }


    @Test
    public void handleRequest_WhenRegistrationRequestFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_NO_ID, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_WITH_NO_IDENTIFIER, glueClient::registerSchemaVersion))
            .thenThrow(InternalServiceException.class);

        Exception exception = assertThrows(
            CfnGeneralServiceException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_REQUEST_WITH_NO_IDENTIFIER,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains("Error occurred during operation ");
    }

    @Test
    public void handleRequest_WhenSchemaIdIsNotProvided_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenThrow(InvalidInputException.class);

        assertThrows(
            CfnInvalidRequestException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );
    }

    @Test
    public void handleRequest_WhenSchemaIdIsInvalid_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN, glueClient::getSchemaByDefinition))
            .thenThrow(EntityNotFoundException.class);

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN, glueClient::registerSchemaVersion))
            .thenThrow(EntityNotFoundException.class);

        Exception exception = assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN,
                new CallbackContext(),
                proxyClient,
                logger
            )
        );

        assertThat(exception.getMessage()).contains("Resource of type 'AWS::Glue::SchemaVersion' with identifier "
            + "'SchemaId(SchemaArn=arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema)' was not found.");
    }


    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String SCHEMA_NAME = "unit-test-schema";
        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        public final static String SCHEMA_VERSION_ID = "yurt9301-dc50-11ea-87d0-8iofb18nkrp8";
        public final static String NEXT_SCHEMA_VERSION_ID = "307ce1bc-dc50-11ea-87d0-0242ac130003";
        public static final String SCHEMA_DEFINITION = "{\"type\": \"fixed\", \"size\": 16, \"name\": \"md5\"}";
        public static final Long NEXT_SCHEMA_VERSION_NUMBER = 2l;

        public static final ResourceModel RESOURCE_MODEL_FOR_VERSION_UPDATE_BY_ARN =
            ResourceModel
                .builder()
                .schema(
                    Schema
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_ARN =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_FOR_VERSION_UPDATE_BY_ARN)
                .build();

        public static final RegisterSchemaVersionRequest REGISTER_SCHEMA_VERSION_REQUEST_BY_ARN =
            RegisterSchemaVersionRequest
                .builder()
                .schemaId(
                    software.amazon.awssdk.services.glue.model.SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();

        public static final ResourceModel RESOURCE_MODEL_FOR_VERSION_BY_NAME =
            ResourceModel
                .builder()
                .schema(
                    Schema
                        .builder()
                        .schemaName(SCHEMA_NAME)
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_MODEL_RESOURCE_HANDLER_FOR_VERSION_BY_NAME =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_FOR_VERSION_BY_NAME)
                .build();

        public static final RegisterSchemaVersionRequest REGISTER_SCHEMA_VERSION_REQUEST_FOR_VERSION_BY_NAME =
            RegisterSchemaVersionRequest
                .builder()
                .schemaId(
                    software.amazon.awssdk.services.glue.model.SchemaId
                        .builder()
                        .schemaName(SCHEMA_NAME)
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();

        public static final RegisterSchemaVersionResponse getRegisterSchemaVersionResponseWithStatus(
            SchemaVersionStatus status) {
            return RegisterSchemaVersionResponse
                .builder()
                .schemaVersionId(NEXT_SCHEMA_VERSION_ID)
                .status(status)
                .versionNumber(NEXT_SCHEMA_VERSION_NUMBER)
                .build();
        }

        public static final GetSchemaVersionRequest GET_SCHEMA_VERSION_REQUEST_BY_SCHEMA_ARN =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(NEXT_SCHEMA_VERSION_ID)
                .build();

        public static final GetSchemaVersionResponse getSchemaVersionResponseWithStatus(SchemaVersionStatus status) {
            return GetSchemaVersionResponse
                .builder()
                .schemaVersionId(NEXT_SCHEMA_VERSION_ID)
                .schemaArn(SCHEMA_ARN)
                .dataFormat(DataFormat.AVRO)
                .status(status)
                .createdTime(Instant.now().toString())
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();
        }

        public static final ResourceModel GET_SCHEMA_VERSION_RESPONSE_RESOURCE_MODEL =
            ResourceModel
                .builder()
                .versionId(NEXT_SCHEMA_VERSION_ID)
                .schemaDefinition(SCHEMA_DEFINITION)
                .schema(
                    Schema
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public static final ResourceModel RESOURCE_MODEL_WITH_NO_IDENTIFIER =
            ResourceModel
                .builder()
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_MODEL_RESOURCE_HANDLER_REQUEST_WITH_NO_IDENTIFIER =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_NO_IDENTIFIER)
                .build();

        public static final RegisterSchemaVersionRequest REGISTER_SCHEMA_VERSION_REQUEST_WITH_NO_IDENTIFIER =
            RegisterSchemaVersionRequest
                .builder()
                .schemaId(
                    software.amazon.awssdk.services.glue.model.SchemaId
                        .builder()
                        .build()
                )
                .schemaDefinition(SCHEMA_DEFINITION)
                .build();


        public static final GetSchemaByDefinitionRequest GET_SCHEMA_BY_DEFINITION_REQUEST_NO_ID =
            GetSchemaByDefinitionRequest
                .builder()
                .schemaDefinition(SCHEMA_DEFINITION)
                .schemaId(
                    SchemaId
                        .builder()
                        .build()
                )
                .build();

        public static final GetSchemaByDefinitionRequest GET_SCHEMA_BY_DEFINITION_REQUEST_BY_ARN =
            GetSchemaByDefinitionRequest
                .builder()
                .schemaDefinition(SCHEMA_DEFINITION)
                .schemaId(
                    SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public static final GetSchemaByDefinitionResponse GET_SCHEMA_BY_DEFINITION_RESPONSE =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_VERSION_ID)
                .build();

        public static final GetSchemaByDefinitionRequest GET_SCHEMA_BY_DEFINITION_REQUEST_BY_NAME =
            GetSchemaByDefinitionRequest
                .builder()
                .schemaDefinition(SCHEMA_DEFINITION)
                .schemaId(
                    SchemaId
                        .builder()
                        .registryName(REGISTRY_NAME)
                        .schemaName(SCHEMA_NAME)
                        .build()
                )
                .build();
    }
}
