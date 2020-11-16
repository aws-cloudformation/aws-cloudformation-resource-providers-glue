package software.amazon.glue.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.SchemaStatus;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    @Mock
    private GlueClient glueClient;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new CreateHandler();
    }

    @Test
    public void handleRequest_CreateSchemaSucceedsWithTags_ReturnSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITH_TAGS,
            glueClient::createSchema)
        ).thenReturn(TestData.CREATE_SCHEMA_RESPONSE_WITH_TAGS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
            new CallbackContext(), proxyClient, logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.CREATE_SCHEMA_RESPONSE_WITH_TAGS_RESOURCE);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_CreateSchemaSucceedsWithoutTags_ReturnSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITHOUT_TAGS,
            glueClient::createSchema)
        ).thenReturn(TestData.CREATE_SCHEMA_RESPONSE_WITHOUT_TAGS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITHOUT_TAGS,
            new CallbackContext(), proxyClient, logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.CREATE_SCHEMA_RESPONSE_WITHOUT_TAGS_RESOURCE);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_CreateSchemaWithNoRegistryName_ReturnSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITH_NO_REGISTRY,
            glueClient::createSchema)
        ).thenReturn(TestData.CREATE_SCHEMA_RESPONSE_WITH_DEFAULT_REGISTRY);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY,
            new CallbackContext(), proxyClient, logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel())
            .isEqualTo(TestData.CREATE_SCHEMA_RESPONSE_WITH_DEFAULT_REGISTRY_RESOURCE);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_CreateSchemaSucceedsWithRegistryArn_ReturnSuccess() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITH_REGISTRY_ARN,
            glueClient::createSchema)
        ).thenReturn(TestData.CREATE_SCHEMA_RESPONSE_WITH_TAGS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy,
            TestData.RESOURCE_HANDLER_REQUEST_WITH_REGISTRY_ARN,
            new CallbackContext(), proxyClient, logger
        );

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TestData.CREATE_SCHEMA_RESPONSE_WITH_TAGS_RESOURCE);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenSchemaAlreadyExists_ThrowsException() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITH_TAGS,
            glueClient::createSchema)
        ).thenThrow(AlreadyExistsException.class);

        Exception exception = assertThrows(CfnAlreadyExistsException.class, () ->
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
                new CallbackContext(), proxyClient, logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Resource of type 'AWS::Glue::Schema' with identifier 'unit-test-schema' already exists.");
    }

    @Test
    public void handleRequest_WhenAccessDenied_ThrowsException() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITH_TAGS,
            glueClient::createSchema)
        ).thenThrow(AccessDeniedException.class);

        Exception exception = assertThrows(CfnAccessDeniedException.class, () ->
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
                new CallbackContext(), proxyClient, logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Access denied for operation 'AWS::Glue::Schema'");
    }

    @Test
    public void handleRequest_WhenResourceLimitExceeded_ThrowsException() {

        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_SCHEMA_REQUEST_WITH_TAGS,
            glueClient::createSchema)
        ).thenThrow(ResourceNumberLimitExceededException.class);

        Exception exception = assertThrows(CfnServiceLimitExceededException.class, () ->
            handler.handleRequest(
                proxy,
                TestData.RESOURCE_HANDLER_REQUEST_WITH_TAGS,
                new CallbackContext(), proxyClient, logger
            )
        );

        assertThat(exception.getMessage())
            .contains("Limit exceeded for resource of type 'AWS::Glue::Schema'.");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";
        public final static String DEFAULT_REGISTRY_NAME = "default-registry";
        public final static String DEFAULT_REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/default-registry";
        public final static String SCHEMA_NAME = "unit-test-schema";
        public final static String SCHEMA_DESC = "Creating a unit-test schema";
        public final static List<Tag> RESOURCE_TAGS =
            ImmutableList.of(new Tag("Project", "Example"), new Tag("Org", "ABC"));
        public final static Map<String, String> TAGS = ImmutableSortedMap.of("Project", "Example", "Org", "ABC");

        private static final String DATA_FORMAT = "Avro";
        private static final String COMPATIBILITY = "BACKWARD_ALL";
        private static final Long LATEST_VERSION = 1l;
        private static final Long NEXT_VERSION = 2l;
        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        private static final Long CHECKPOINT_VERSION = 1l;
        public static final String SCHEMA_DEFINITION = "{\"type\": \"fixed\", \"size\": 16, \"name\": \"md5\"}";
        private static final String SCHEMA_VERSION_ID = "123e4567-e89b-12d3-a456-426614174000";

        public final static ResourceModel
            RESOURCE_MODEL_WITH_TAGS = ResourceModel
            .builder()
            .name(SCHEMA_NAME)
            .registry(
                Registry
                    .builder()
                    .name(REGISTRY_NAME)
                    .build()
            )
            .description(SCHEMA_DESC)
            .dataFormat(DATA_FORMAT)
            .schemaDefinition(SCHEMA_DEFINITION)
            .compatibility(COMPATIBILITY)
            .tags(RESOURCE_TAGS)
            .build();

        public final static ResourceModel
            RESOURCE_MODEL_WITH_REGISTRY_ARN = ResourceModel
            .builder()
            .name(SCHEMA_NAME)
            .registry(
                Registry
                    .builder()
                    .arn(REGISTRY_ARN)
                    .build()
            )
            .description(SCHEMA_DESC)
            .dataFormat(DATA_FORMAT)
            .schemaDefinition(SCHEMA_DEFINITION)
            .compatibility(COMPATIBILITY)
            .tags(RESOURCE_TAGS)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_REGISTRY_ARN =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_REGISTRY_ARN)
                .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_TAGS =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_TAGS)
                .build();

        public final static CreateSchemaRequest CREATE_SCHEMA_REQUEST_WITH_TAGS =
            CreateSchemaRequest.builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .description(SCHEMA_DESC)
                .dataFormat(DATA_FORMAT)
                .schemaName(SCHEMA_NAME)
                .compatibility(COMPATIBILITY)
                .schemaDefinition(SCHEMA_DEFINITION)
                .tags(TAGS)
                .build();

        public final static CreateSchemaRequest CREATE_SCHEMA_REQUEST_WITH_REGISTRY_ARN =
            CreateSchemaRequest.builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryArn(REGISTRY_ARN)
                        .build()
                )
                .description(SCHEMA_DESC)
                .dataFormat(DATA_FORMAT)
                .schemaName(SCHEMA_NAME)
                .compatibility(COMPATIBILITY)
                .schemaDefinition(SCHEMA_DEFINITION)
                .tags(TAGS)
                .build();

        public final static ResourceModel
            RESOURCE_MODEL_WITHOUT_TAGS = ResourceModel
            .builder()
            .name(SCHEMA_NAME)
            .registry(
                Registry
                    .builder()
                    .name(REGISTRY_NAME)
                    .build()
            )
            .description(SCHEMA_DESC)
            .dataFormat(DATA_FORMAT)
            .schemaDefinition(SCHEMA_DEFINITION)
            .compatibility(COMPATIBILITY)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITHOUT_TAGS =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITHOUT_TAGS)
                .build();

        public final static CreateSchemaRequest CREATE_SCHEMA_REQUEST_WITHOUT_TAGS =
            CreateSchemaRequest.builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .description(SCHEMA_DESC)
                .dataFormat(DATA_FORMAT)
                .schemaName(SCHEMA_NAME)
                .compatibility(COMPATIBILITY)
                .schemaDefinition(SCHEMA_DEFINITION)
                .tags(Collections.unmodifiableMap(Collections.emptyMap()))
                .build();

        public final static CreateSchemaResponse CREATE_SCHEMA_RESPONSE_WITHOUT_TAGS =
            CreateSchemaResponse.builder()
                .compatibility(COMPATIBILITY)
                .description(SCHEMA_DESC)
                .latestSchemaVersion(LATEST_VERSION)
                .nextSchemaVersion(NEXT_VERSION)
                .registryArn(REGISTRY_ARN)
                .registryName(REGISTRY_NAME)
                .schemaCheckpoint(CHECKPOINT_VERSION)
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .dataFormat(DATA_FORMAT)
                .schemaVersionId(SCHEMA_VERSION_ID)
                .tags(Collections.emptyMap())
                .build();

        public final static CreateSchemaResponse CREATE_SCHEMA_RESPONSE_WITH_TAGS =
            CreateSchemaResponse.builder()
                .compatibility(COMPATIBILITY)
                .description(SCHEMA_DESC)
                .latestSchemaVersion(LATEST_VERSION)
                .nextSchemaVersion(NEXT_VERSION)
                .registryArn(REGISTRY_ARN)
                .registryName(REGISTRY_NAME)
                .schemaArn(SCHEMA_ARN)
                .schemaCheckpoint(CHECKPOINT_VERSION)
                .schemaName(SCHEMA_NAME)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .schemaVersionId(SCHEMA_VERSION_ID)
                .dataFormat(DATA_FORMAT)
                .tags(TAGS)
                .build();

        public final static ResourceModel CREATE_SCHEMA_RESPONSE_WITH_TAGS_RESOURCE =
            ResourceModel
                .builder()
                .name(TestData.SCHEMA_NAME)
                .description(TestData.SCHEMA_DESC)
                .compatibility(TestData.COMPATIBILITY)
                .dataFormat(TestData.DATA_FORMAT)
                .registry(Registry.builder().arn(TestData.REGISTRY_ARN).build())
                .arn(TestData.SCHEMA_ARN)
                .tags(TestData.RESOURCE_TAGS)
                .initialSchemaVersionId(SCHEMA_VERSION_ID)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .versionNumber(LATEST_VERSION.intValue())
                        .isLatest(true)
                        .build()
                )
                .build();

        public final static ResourceModel CREATE_SCHEMA_RESPONSE_WITHOUT_TAGS_RESOURCE =
            ResourceModel
                .builder()
                .name(TestData.SCHEMA_NAME)
                .description(TestData.SCHEMA_DESC)
                .compatibility(TestData.COMPATIBILITY)
                .dataFormat(TestData.DATA_FORMAT)
                .registry(Registry.builder().arn(TestData.REGISTRY_ARN).build())
                .arn(TestData.SCHEMA_ARN)
                .tags(Collections.emptyList())
                .initialSchemaVersionId(SCHEMA_VERSION_ID)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .versionNumber(LATEST_VERSION.intValue())
                        .isLatest(true)
                        .build()
                )
                .build();

        public final static ResourceModel
            RESOURCE_MODEL_WITH_NO_REGISTRY = ResourceModel
            .builder()
            .name(SCHEMA_NAME)
            .description(SCHEMA_DESC)
            .dataFormat(DATA_FORMAT)
            .schemaDefinition(SCHEMA_DEFINITION)
            .compatibility(COMPATIBILITY)
            .build();

        public final static ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_NO_REGISTRY)
                .build();

        public final static CreateSchemaRequest CREATE_SCHEMA_REQUEST_WITH_NO_REGISTRY =
            CreateSchemaRequest.builder()
                .description(SCHEMA_DESC)
                .dataFormat(DATA_FORMAT)
                .schemaName(SCHEMA_NAME)
                .schemaDefinition(SCHEMA_DEFINITION)
                .compatibility(COMPATIBILITY)
                .tags(Collections.unmodifiableMap(Collections.emptyMap()))
                .build();

        public final static CreateSchemaResponse CREATE_SCHEMA_RESPONSE_WITH_DEFAULT_REGISTRY =
            CreateSchemaResponse.builder()
                .compatibility(COMPATIBILITY)
                .description(SCHEMA_DESC)
                .latestSchemaVersion(LATEST_VERSION)
                .nextSchemaVersion(NEXT_VERSION)
                .registryArn(DEFAULT_REGISTRY_ARN)
                .registryName(DEFAULT_REGISTRY_NAME)
                .schemaCheckpoint(CHECKPOINT_VERSION)
                .schemaName(SCHEMA_NAME)
                .schemaVersionId(SCHEMA_VERSION_ID)
                .schemaArn(SCHEMA_ARN)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .dataFormat(DATA_FORMAT)
                .schemaCheckpoint(LATEST_VERSION)
                .tags(Collections.emptyMap())
                .build();

        public final static ResourceModel CREATE_SCHEMA_RESPONSE_WITH_DEFAULT_REGISTRY_RESOURCE =
            ResourceModel
                .builder()
                .name(TestData.SCHEMA_NAME)
                .description(TestData.SCHEMA_DESC)
                .compatibility(TestData.COMPATIBILITY)
                .dataFormat(TestData.DATA_FORMAT)
                .registry(
                    Registry
                        .builder()
                        .arn(TestData.DEFAULT_REGISTRY_ARN)
                        .build()
                )
                .initialSchemaVersionId(SCHEMA_VERSION_ID)
                .arn(TestData.SCHEMA_ARN)
                .checkpointVersion(
                    SchemaVersion
                        .builder()
                        .versionNumber(LATEST_VERSION.intValue())
                        .isLatest(true)
                        .build()
                )
                .tags(Collections.emptyList())
                .build();
    }

}
