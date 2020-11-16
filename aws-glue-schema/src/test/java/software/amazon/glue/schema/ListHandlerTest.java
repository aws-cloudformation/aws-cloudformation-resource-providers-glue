package software.amazon.glue.schema;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.ListSchemasRequest;
import software.amazon.awssdk.services.glue.model.ListSchemasResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.SchemaListItem;
import software.amazon.awssdk.services.glue.model.SchemaStatus;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<GlueClient> proxyClient;

    @Mock
    private GlueClient glueClient;

    @Mock
    private Logger logger;

    private ListHandler handler;

    @BeforeEach
    public void setup() {
        proxy = getAmazonWebServicesClientProxy();
        proxyClient = MOCK_PROXY(proxy, glueClient);
        handler = new ListHandler();
    }

    @Test
    public void handleRequest_WhenInvokedWithRegistryName_ReturnsResultsSuccessfully() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMAS_REQUEST_WITH_NAME, glueClient::listSchemas))
            .thenReturn(TestData.LIST_SCHEMAS_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST_WITH_NAME, new CallbackContext(), proxyClient, logger);

        //Assert the response.
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.SCHEMA_MODELS);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenInvokedWithRegistryArn_ReturnsResultsSuccessfully() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMAS_REQUEST_WITH_ARN, glueClient::listSchemas))
            .thenReturn(TestData.LIST_SCHEMAS_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST_WITH_ARN, new CallbackContext(), proxyClient, logger);

        //Assert the response.
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.SCHEMA_MODELS);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenInvokedWithNoRegistry_ReturnsResultsSuccessfully() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMAS_REQUEST_WITH_NO_REGISTRY, glueClient::listSchemas))
            .thenReturn(TestData.LIST_SCHEMAS_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY, new CallbackContext(), proxyClient, logger);

        //Assert the response.
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.SCHEMA_MODELS);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenListRegistriesFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMAS_REQUEST_WITH_NO_REGISTRY, glueClient::listSchemas))
            .thenThrow(InternalServiceException.class);


        Exception exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler.handleRequest(
                    proxy, TestData.RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY, new CallbackContext(), proxyClient, logger)
            );

        assertThat(exception.getMessage())
            .contains("Error occurred during operation ");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String REGISTRY_ARN = "arn:aws:glue:us-east-1:123456789:registry/unit-testing-registry";

        public final static String SCHEMA_NAME = "unit-test-schema";
        public final static String ANOTHER_SCHEMA_NAME = "unit-test-schema-2";

        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        private static final String ANOTHER_SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema-2";

        public static final String NEXT_TOKEN = "091230123921=";
        public static final String ANOTHER_NEXT_TOKEN = "sdf434g";

        private static final String UPDATED_TIME = "12345900";
        private static final String CREATED_TIME = "9098012321";

        private static final Integer MAX_RESULTS = 50;

        public static final ListSchemasRequest LIST_SCHEMAS_REQUEST_WITH_NAME =
            ListSchemasRequest
                .builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .nextToken(NEXT_TOKEN)
                .maxResults(MAX_RESULTS)
                .build();

        public static final ListSchemasRequest LIST_SCHEMAS_REQUEST_WITH_ARN =
            ListSchemasRequest
                .builder()
                .registryId(
                    RegistryId
                        .builder()
                        .registryArn(REGISTRY_ARN)
                        .build()
                )
                .nextToken(NEXT_TOKEN)
                .maxResults(MAX_RESULTS)
                .build();

        private static final ResourceModel RESOURCE_MODEL_WITH_NAME =
            ResourceModel
                .builder()
                .registry(
                    software.amazon.glue.schema.Registry
                        .builder()
                        .name(REGISTRY_NAME)
                        .build()
                )
                .build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_NAME =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_NAME)
                .nextToken(NEXT_TOKEN)
                .build();

        private static final ResourceModel RESOURCE_MODEL_WITH_ARN =
            ResourceModel
                .builder()
                .registry(
                    software.amazon.glue.schema.Registry
                        .builder()
                        .arn(REGISTRY_ARN)
                        .build()
                )
                .build();
        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_ARN =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_ARN)
                .nextToken(NEXT_TOKEN)
                .build();

        public static final ListSchemasRequest LIST_SCHEMAS_REQUEST_WITH_NO_REGISTRY =
            ListSchemasRequest
                .builder()
                .nextToken(NEXT_TOKEN)
                .maxResults(MAX_RESULTS)
                .build();

        private static final ResourceModel RESOURCE_MODEL_WITH_NO_REGISTRY =
            ResourceModel
                .builder()
                .build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_WITH_NO_REGISTRY =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_WITH_NO_REGISTRY)
                .nextToken(NEXT_TOKEN)
                .build();

        private static final ResourceModel RESOURCE_MODEL_1 =
            ResourceModel
                .builder()
                .name(SCHEMA_NAME)
                .arn(SCHEMA_ARN)
                .build();

        private static final ResourceModel RESOURCE_MODEL_2 =
            ResourceModel
                .builder()
                .name(ANOTHER_SCHEMA_NAME)
                .arn(ANOTHER_SCHEMA_ARN)
                .build();

        public static final List<ResourceModel> SCHEMA_MODELS =
            ImmutableList.of(RESOURCE_MODEL_1, RESOURCE_MODEL_2);

        private final static SchemaListItem SCHEMA_LIST_ITEM_1 =
            SchemaListItem
                .builder()
                .schemaName(SCHEMA_NAME)
                .schemaArn(SCHEMA_ARN)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .createdTime(CREATED_TIME)
                .updatedTime(UPDATED_TIME)
                .build();

        private final static SchemaListItem SCHEMA_LIST_ITEM_2 =
            SchemaListItem
                .builder()
                .schemaName(ANOTHER_SCHEMA_NAME)
                .schemaArn(ANOTHER_SCHEMA_ARN)
                .schemaStatus(SchemaStatus.AVAILABLE)
                .createdTime(CREATED_TIME)
                .updatedTime(UPDATED_TIME)
                .build();

        private static final List<SchemaListItem> SCHEMAS = ImmutableList.of(SCHEMA_LIST_ITEM_1, SCHEMA_LIST_ITEM_2);

        public final static ListSchemasResponse LIST_SCHEMAS_RESPONSE = ListSchemasResponse
            .builder()
            .nextToken(TestData.ANOTHER_NEXT_TOKEN)
            .schemas(TestData.SCHEMAS)
            .build();
    }
}
