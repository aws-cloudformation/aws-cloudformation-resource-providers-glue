package software.amazon.glue.schemaversion;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsRequest;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsResponse;
import software.amazon.awssdk.services.glue.model.SchemaVersionListItem;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Instant;
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
    public void handleRequest_WhenInvokedBySchemaName_ReturnsResults() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMA_VERSIONS_REQUEST_BY_NAME, glueClient::listSchemaVersions))
        .thenReturn(TestData.LIST_SCHEMA_VERSIONS_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_BY_SCHEMA_NAME, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.RESOURCE_MODEL_LIST);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenInvokedBySchemaArn_ReturnsResults() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMA_VERSIONS_REQUEST_BY_ARN, glueClient::listSchemaVersions))
            .thenReturn(TestData.LIST_SCHEMA_VERSIONS_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_BY_SCHEMA_ARN, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.RESOURCE_MODEL_LIST);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenNoSchemaIdIsProvided_ReturnsResults() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMA_VERSIONS_REQUEST_BY_NO_ID, glueClient::listSchemaVersions))
            .thenReturn(TestData.LIST_SCHEMA_VERSIONS_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_BY_NO_ID, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isEqualTo(TestData.RESOURCE_MODEL_LIST);
        assertThat(response.getNextToken()).isEqualTo(TestData.ANOTHER_NEXT_TOKEN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_WhenListCallFails_ThrowsException() {

        when(proxy.injectCredentialsAndInvokeV2(
            TestData.LIST_SCHEMA_VERSIONS_REQUEST_BY_NO_ID, glueClient::listSchemaVersions))
            .thenThrow(AccessDeniedException.class);

        final Exception exception =
            assertThrows(
                CfnAccessDeniedException.class,
                () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_BY_NO_ID, new CallbackContext(), proxyClient, logger)
            );

        assertThat(exception.getMessage())
            .contains("Access denied for operation 'AWS::Glue::SchemaVersion'");
    }

    private static class TestData {
        public final static String REGISTRY_NAME = "unit-test-registry";
        public final static String SCHEMA_NAME = "unit-test-schema";
        private static final String SCHEMA_ARN =
            "arn:aws:glue:us-east-1:123456789:schema/unit-testing-registry/unit-testing-schema";
        public final static String SCHEMA_VERSION_ID_1 = "307ce1bc-dc50-11ea-87d0-0242ac130003";
        public final static String SCHEMA_VERSION_ID_2 = "8078kior-dc50-11ea-87d0-0242ac130003";
        private static final Long VERSION_NUMBER_1 = 1l;
        private static final Long VERSION_NUMBER_2 = 2l;
        public static final String ANOTHER_NEXT_TOKEN = "09j09j09123o=";
        private static final Integer MAX_RESULTS = 50;

        public static final ResourceModel RESOURCE_MODEL_BY_SCHEMA_ARN =
            ResourceModel
                .builder()
                .schema(
                    Schema
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .build();

        public static final ResourceHandlerRequest<ResourceModel>
            RESOURCE_HANDLER_BY_SCHEMA_ARN =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BY_SCHEMA_ARN)
                .build();

        public static final ListSchemaVersionsRequest LIST_SCHEMA_VERSIONS_REQUEST_BY_ARN =
            ListSchemaVersionsRequest
                .builder()
                .schemaId(
                    software.amazon.awssdk.services.glue.model.SchemaId
                        .builder()
                        .schemaArn(SCHEMA_ARN)
                        .build()
                )
                .maxResults(MAX_RESULTS)
                .build();

        public static final ResourceModel RESOURCE_MODEL_BY_SCHEMA_NAME =
            ResourceModel
                .builder()
                .schema(
                    Schema
                        .builder()
                        .schemaName(SCHEMA_NAME)
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .build();

        public static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_BY_SCHEMA_NAME =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BY_SCHEMA_NAME)
                .build();

        public static final ListSchemaVersionsRequest LIST_SCHEMA_VERSIONS_REQUEST_BY_NAME =
            ListSchemaVersionsRequest
                .builder()
                .schemaId(
                    software.amazon.awssdk.services.glue.model.SchemaId
                        .builder()
                        .schemaName(SCHEMA_NAME)
                        .registryName(REGISTRY_NAME)
                        .build()
                )
                .maxResults(MAX_RESULTS)
                .build();

        public static final ResourceModel RESOURCE_MODEL_BY_NO_ID =
            ResourceModel
                .builder()
                .build();

        public static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_BY_NO_ID =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_BY_NO_ID)
                .build();

        public static final ListSchemaVersionsRequest LIST_SCHEMA_VERSIONS_REQUEST_BY_NO_ID =
            ListSchemaVersionsRequest
                .builder()
                .maxResults(MAX_RESULTS)
                .build();

        public static final SchemaVersionListItem SCHEMA_VERSION_LIST_ITEM_1 =
            SchemaVersionListItem
                .builder()
                .createdTime(Instant.now().toString())
                .schemaVersionId(SCHEMA_VERSION_ID_1)
                .versionNumber(VERSION_NUMBER_1)
                .build();

        public static final SchemaVersionListItem SCHEMA_VERSION_LIST_ITEM_2 =
            SchemaVersionListItem
                .builder()
                .createdTime(Instant.now().toString())
                .schemaVersionId(SCHEMA_VERSION_ID_2)
                .versionNumber(VERSION_NUMBER_2)
                .build();

        public static final ListSchemaVersionsResponse LIST_SCHEMA_VERSIONS_RESPONSE =
            ListSchemaVersionsResponse
                .builder()
                .nextToken(ANOTHER_NEXT_TOKEN)
                .schemas(
                    ImmutableList.of(
                        SCHEMA_VERSION_LIST_ITEM_1,
                        SCHEMA_VERSION_LIST_ITEM_2
                    )
                )
                .build();

        private static final ResourceModel LIST_RESOURCE_MODEL_1 =
            ResourceModel
                .builder()
                .versionId(SCHEMA_VERSION_ID_1)
                .build();

        private static final ResourceModel LIST_RESOURCE_MODEL_2 =
            ResourceModel
                .builder()
                .versionId(SCHEMA_VERSION_ID_2)
                .build();

        public static final List<ResourceModel> RESOURCE_MODEL_LIST =
            ImmutableList.of(LIST_RESOURCE_MODEL_1, LIST_RESOURCE_MODEL_2);
    }

}
