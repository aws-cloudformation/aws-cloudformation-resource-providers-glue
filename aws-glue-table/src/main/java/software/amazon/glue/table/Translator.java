package software.amazon.glue.table;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.TableInput;

public class Translator {

    /**
     * Translates the resource model object to the corresponding service request object for the Read operation.
     *
     * @param model The resource model object.
     * @return The corresponding service request object for the Read operation.
     */
    static GetTableRequest translateToReadRequest(final ResourceModel model) {
        // Validate the input model
        if (model == null || model.getDatabaseName() == null || model.getTableName() == null) {
            throw new IllegalArgumentException("Invalid input model");
        }

        return GetTableRequest.builder()
                .databaseName(model.getDatabaseName())
                .name(model.getTableName())
                .build();
    }

    /**
     * Translates the service response object to the corresponding resource model object for the Read operation.
     *
     * @param response: The GetTableResponse from the Glue SDK
     * @return The corresponding resource model object for the Read operation.
     */
    static ResourceModel translateFromReadResponse(final GetTableResponse response) {
        // Validate the input response
        if(response == null) return null;

        return ResourceModel.builder()
                .databaseName(response.table().databaseName())
                .catalogId(response.table().catalogId())
                .tableName(response.table().name())
                //.openTableFormatInput(TranslatorUtils.translateOpenTableFormatInput(response.table().openTableFormatInput()))
                .tableInput(TranslatorUtils.translateTableInputFromSDK(response.table()))
                .build();
    }

    /**
     * Translates the resource model object to the corresponding service request object for the Create operation.
     *
     * @param resourceModel The resource model object.
     * @return The corresponding service request object for the Create operation.
     */
    public static CreateTableRequest translateToCreateRequest(final ResourceModel resourceModel) {
        //final String tableName = resourceModel.getTableName();
        final TableInput tableInput = TranslatorUtils.translateToSDKTableInput(resourceModel.getTableInput());
        return CreateTableRequest.builder()
                .databaseName(resourceModel.getDatabaseName())
                .catalogId(resourceModel.getCatalogId())
                .openTableFormatInput(TranslatorUtils.translateToSdkOpenTableFormatInput(resourceModel.getOpenTableFormatInput()))
                .tableInput(tableInput)
                .build();
    }

    static GetTablesRequest translateToListRequest(final String nextToken, final String databaseName) {
        return GetTablesRequest.builder()
                .databaseName(databaseName)
                .nextToken(nextToken)
                .build();
    }

    static List<ResourceModel> translateFromListResponse( final GetTablesResponse listTablesResponse, final String databaseName) {
        final List<ResourceModel> resourceModels = new ArrayList<>();

        if(listTablesResponse.hasTableList()) {
            for (Table table : listTablesResponse.tableList()) {
                resourceModels.add(ResourceModel.builder()
                        .catalogId(table.catalogId())
                        .databaseName(databaseName)
                        .tableName(table.name())
                        .tableInput(TranslatorUtils.translateTableInputFromSDK(table))
                        .build());
            }
        }
        return resourceModels;
    }

    static UpdateTableRequest translateToUpdateRequest(final ResourceModel model) {
        final TableInput tableInput = TranslatorUtils.translateToSDKTableInput(model.getTableInput());
    final UpdateTableRequest.Builder builder = UpdateTableRequest.builder()
        .databaseName(model.getDatabaseName())
        .catalogId(model.getCatalogId())
        .tableInput(tableInput);

        return builder.build();
    }


    protected static ResourceModel translateFromUpdateResponse(final UpdateTableResponse response, final ResourceModel model) {
        return ResourceModel.builder()
            .databaseName(model.getDatabaseName())
            .catalogId(model.getCatalogId())
            .tableInput(model.getTableInput())
            .tableName(model.getTableName())
            .build();
    }

    protected static DeleteTableRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteTableRequest.builder()
                .databaseName(model.getDatabaseName())
                .name(model.getTableName())
                .build();
    }
}
