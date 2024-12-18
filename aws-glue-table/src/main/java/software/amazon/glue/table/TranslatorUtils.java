package software.amazon.glue.table;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.IcebergInput;
import software.amazon.awssdk.services.glue.model.OpenTableFormatInput;
import software.amazon.awssdk.services.glue.model.MetadataOperation;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaReference;
import software.amazon.awssdk.services.glue.model.SkewedInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableIdentifier;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Collectors;

public class TranslatorUtils {

    private TranslatorUtils() {
        throw new IllegalArgumentException("Utility class");
    }

    /**
     * Translates the SDK property object to the corresponding resource model object.
     *
     * @param sdkObject The SDK property object.
     * @return The corresponding resource model object.
     */
    static software.amazon.glue.table.TableInput translateTableInputFromSDK(final Table sdkObject) {
        // Validate the input SDK object
        if (sdkObject == null) {
            return null;
        }

        return software.amazon.glue.table.TableInput.builder()
                .owner(sdkObject.owner())
                .viewOriginalText(sdkObject.viewOriginalText())
                .description(sdkObject.description())
                .tableType(sdkObject.tableType())
                .parameters(translateParametersFromSDK(sdkObject.parameters()))
                .viewExpandedText(sdkObject.viewExpandedText())
                .storageDescriptor(translateStorageDescriptorFromSDK(sdkObject.storageDescriptor()))
                .targetTable(translateTableIdentifierFromSDK(sdkObject.targetTable()))
                .partitionKeys(translatePartitionKeysFromSDK(sdkObject.partitionKeys()))
                .retention(sdkObject.retention())
                .name(sdkObject.name())
                .build();
    }


    /**
     * Translates a list of Glue SDK Column into a list of ResourceModel Columns
     *
     * @param sdkObject: The list of Glue SDK column object
     * @return The list of ResourceModel column object
     */
    static List<software.amazon.glue.table.Column> translatePartitionKeysFromSDK(final List<Column> sdkObject) {
        if(sdkObject == null) return null;
        else {
            return sdkObject.stream().map(columnSdkObject -> {
                return software.amazon.glue.table.Column.builder()
                        .comment(columnSdkObject.comment())
                        .name(columnSdkObject.name())
                        .type(columnSdkObject.type())
                        .build();
            }).collect(Collectors.toList());
        }
    }


    /**
     * Translates a Glue SDK Map<String,String> into resource model Map<String,Object> object
     *
     * @param parameters: The CFN Glue object
     * @return The Glue SDK object
     */
    static Map<String, Object> translateParametersFromSDK(final Map<String, String> parameters) {
        return parameters == null ?
                Collections.emptyMap() :
                parameters.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> (Object)e.getValue()));
    }

    /**
     * Translates a resource model Map<String,String> into Glue SDK Map<String,Object> object
     *
     * @param parameters: The CFN Glue object
     * @return The Glue SDK object
     */
    static Map<String, String> translateToSDKParameters(final Map<String, Object> parameters) {
        return parameters == null ?
                Collections.emptyMap() :
                parameters.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> (String)e.getValue()));
    }

    /**
     * Translates a Glue SDK StorageDescriptor into a StorageDescriptor ResourceModel object
     *
     * @param sdkObject: The Glue SDK StorageDescriptor object
     * @return The StorageDescriptor ResourceModel object
     */
    static software.amazon.glue.table.StorageDescriptor translateStorageDescriptorFromSDK(final StorageDescriptor sdkObject) {
        if (sdkObject == null) {
            return null;
        }

        return software.amazon.glue.table.StorageDescriptor.builder()
                .columns(translateColumnFromSDK(sdkObject.columns()))
                .location(sdkObject.location())
                .inputFormat(sdkObject.inputFormat())
                .outputFormat(sdkObject.outputFormat())
                .compressed(sdkObject.compressed())
                .numberOfBuckets(sdkObject.numberOfBuckets())
                .serdeInfo(translateSerDeInfoFromSDK(sdkObject.serdeInfo()))
                .bucketColumns(sdkObject.bucketColumns())
                .sortColumns(translateOrderFromSDK(sdkObject.sortColumns()))
                .storedAsSubDirectories(sdkObject.storedAsSubDirectories())
                .parameters(translateParametersFromSDK(sdkObject.parameters()))
                .skewedInfo(translateSkewedInfoFromSDK(sdkObject.skewedInfo()))
                .schemaReference(translateSchemaReferenceFromSDK(sdkObject.schemaReference()))
                .build();
    }

    /**
     * Translates a list of Glue SDK Order into a list of ResourceModel Orders
     *
     * @param sdkObject: The list of Glue SDK order object
     * @return The list of ResourceModel Order object
     */
    static List<software.amazon.glue.table.Order> translateOrderFromSDK(final List<Order> sdkObject) {
        if(sdkObject == null) return Collections.emptyList();
        else {
            return sdkObject.stream().map(orderSdkObject -> {
                return software.amazon.glue.table.Order.builder()
                        .sortOrder(orderSdkObject.sortOrder())
                        .column(orderSdkObject.column())
                        .build();
            }).collect(Collectors.toList());
        }
    }

    /**
     * Translates a list of resource model Order into a list of Glue SDK Orders
     *
     * @param model: The list of resource model order object
     * @return The list of Glue SDK Order object
     */
    static List<Order> translateToSDKOrder(final List<software.amazon.glue.table.Order> model) {
        if(model == null) return null;
        else {
            return model.stream().map(order -> {
                return Order.builder()
                        .sortOrder(order.getSortOrder())
                        .column(order.getColumn())
                        .build();
            }).collect(Collectors.toList());
        }
    }

    /**
     * Translates a list of Glue SDK Column into a list of ResourceModel Columns
     *
     * @param sdkObject: The list of Glue SDK column object
     * @return The list of ResourceModel column object
     */
    static List<software.amazon.glue.table.Column> translateColumnFromSDK(final List<Column> sdkObject) {
        if(sdkObject == null) return null;
        else {
            return sdkObject.stream().map(columnSdkObject -> {
                return software.amazon.glue.table.Column.builder()
                        .comment(columnSdkObject.comment())
                        .name(columnSdkObject.name())
                        .type(columnSdkObject.type())
                        .build();
            }).collect(Collectors.toList());
        }
    }

    /**
     * Translates a Glue SDK SerdeInfo into ResourceModel SerdeInfo
     *
     * @param sdkObject: The Glue SDK SerdeInfo object
     * @return The ResourceModel SerdeInfo object
     */
    static software.amazon.glue.table.SerdeInfo translateSerDeInfoFromSDK(final SerDeInfo sdkObject) {
        if (sdkObject == null) {
            return null;
        }

        return software.amazon.glue.table.SerdeInfo.builder()
                .name(sdkObject.name())
                .serializationLibrary(sdkObject.serializationLibrary())
                .parameters(translateParametersFromSDK(sdkObject.parameters()))
                .build();
    }

    /**
     * Translates a Glue SDK SchemaReference into ResourceModel SchemaReference
     *
     * @param sdkObject: The Glue SDK SchemaReference object
     * @return The ResourceModel SchemaReference object
     */
    static software.amazon.glue.table.SchemaReference translateSchemaReferenceFromSDK(final SchemaReference sdkObject) {
        if (sdkObject == null) {
            return null;
        }

        if (sdkObject.schemaId() == null) {
            if (sdkObject.schemaVersionNumber() == null) {
                return software.amazon.glue.table.SchemaReference.builder()
                    .schemaVersionId(sdkObject.schemaVersionId())
                    .build();
            } else {
                return software.amazon.glue.table.SchemaReference.builder()
                    .schemaVersionId(sdkObject.schemaVersionId())
                    .schemaVersionNumber(Math.toIntExact(sdkObject.schemaVersionNumber()))
                    .build();
            }
        } else if (sdkObject.schemaVersionId() == null) {
            if (sdkObject.schemaVersionNumber() == null) {
                return software.amazon.glue.table.SchemaReference.builder()
                    .schemaId(translateSchemaIdFromSDK(sdkObject.schemaId()))
                    .build();
            } else {
                return software.amazon.glue.table.SchemaReference.builder()
                    .schemaId(translateSchemaIdFromSDK(sdkObject.schemaId()))
                    .schemaVersionNumber(Math.toIntExact(sdkObject.schemaVersionNumber()))
                    .build();
            }
        } else {
            throw new IllegalArgumentException("Either schemaVersionId or schemaId must be provided, but not both");
        }
    }

    /**
     * Translates a Glue SDK SchemaId into ResourceModel SchemaId
     *
     * @param sdkObject: The Glue SDK SchemaId object
     * @return The ResourceModel SchemaId object
     */
    static software.amazon.glue.table.SchemaId translateSchemaIdFromSDK(final SchemaId sdkObject) {
        if (sdkObject == null) {
            return null;
        }

        if (sdkObject.schemaArn() == null) {
            return software.amazon.glue.table.SchemaId.builder()
                .schemaName(sdkObject.schemaName())
                .registryName(sdkObject.registryName())
                .build();
        } else if (sdkObject.schemaName() == null) {
            return software.amazon.glue.table.SchemaId.builder()
                .schemaArn(sdkObject.schemaArn())
                .registryName(sdkObject.registryName())
                .build();
        } else {
            throw new IllegalArgumentException("Either schemaName or schemaArn must be provided, but not both");
        }
    }

    /**
     * Translates a Glue SDK SkewedInfo into ResourceModel SkewedInfo
     *
     * @param sdkObject: The Glue SDK SkewedInfo object
     * @return The ResourceModel SkewedInfo object
     */
    static software.amazon.glue.table.SkewedInfo translateSkewedInfoFromSDK(final SkewedInfo sdkObject) {
        if (sdkObject == null) {
            return null;
        }

        return software.amazon.glue.table.SkewedInfo.builder()
                .skewedColumnNames(sdkObject.skewedColumnNames())
                .skewedColumnValues(sdkObject.skewedColumnValues())
                .skewedColumnValueLocationMaps(translateSkewedColumnValueLocationMapsSDK(sdkObject.skewedColumnValueLocationMaps()))
                .build();
    }

    /**
     * Translates a Glue SDK Map<String,String> skewedColumnValueLocationMaps
     * into resource model Map<String,Object> object
     *
     * @param skewedColumnValueLocationMaps: The CFN Glue object
     * @return The Glue SDK object
     */
    static Map<String, Object> translateSkewedColumnValueLocationMapsSDK(final Map<String, String> skewedColumnValueLocationMaps) {
        return skewedColumnValueLocationMaps == null || skewedColumnValueLocationMaps.isEmpty() ?
                Collections.emptyMap() :
                skewedColumnValueLocationMaps.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()));
    }

    /**
     * Translates a resource model Map<String,Object> skewedColumnValueLocationMaps
     * into Glue SDK Map<String,Object> object
     *
     * @param skewedColumnValueLocationMaps: The resource model Map
     * @return The Glue SDK object
     */
    static Map<String, String> translateToSDKSkewedColumnValueLocationMaps(final Map<String, Object> skewedColumnValueLocationMaps) {
        return skewedColumnValueLocationMaps == null ?
                null :
                skewedColumnValueLocationMaps.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> (String)e.getValue()));
    }

    /**
     * Translates a Glue SDK TableIdentifier into ResourceModel TableIdentifier
     *
     * @param sdkObject: The Glue SDK TableIdentifier object
     * @return The ResourceModel TableIdentifier object
     */
    static software.amazon.glue.table.TableIdentifier translateTableIdentifierFromSDK(final TableIdentifier sdkObject) {
        if (sdkObject == null) {
            return null;
        }

        return software.amazon.glue.table.TableIdentifier.builder()
                .catalogId(sdkObject.catalogId())
                .databaseName(sdkObject.databaseName())
                .region(sdkObject.region())
                .name(sdkObject.name())
                .build();
    }

    /**
     * Translates a list of resource model columns to a list of Glue SDK column
     *
     * @param model: The list of resource model columns
     * @return The list of Glue SDK columns
     */
    static List<Column> translateToSDKColumn(final List<software.amazon.glue.table.Column> model) {
        if(model == null || model.isEmpty()) return null;
        else {
            return model.stream().map(column -> {
                return Column.builder()
                        .comment(column.getComment())
                        .name(column.getName())
                        .type(column.getType())
                        .build();
            }).collect(Collectors.toList());
        }
    }

    /**
     * Translates the resource model TableIdentifier property object to the corresponding
     * SDK TableIdentifier object.
     *
     * @param model The resource model TableIdentifier object.
     * @return The corresponding Glue SDK TableIdentifier object.
     */
    static TableIdentifier translateToSDKTableIdentifier(final software.amazon.glue.table.TableIdentifier model) {
        if (model == null) {
            return null;
        }

        return software.amazon.awssdk.services.glue.model.TableIdentifier.builder()
                .catalogId(model.getCatalogId())
                .databaseName(model.getDatabaseName())
                .region(model.getRegion())
                .name(model.getName())
                .build();
    }

    /**
     * Translates the resource TableInput model object to the corresponding
     * Glue SDK TableInput object.
     *
     * @param model The resource model TableInput object.
     * @return The corresponding Glue SDK TableInput object.
     */
    static TableInput translateToSDKTableInput(final software.amazon.glue.table.TableInput model) {
        // Validate the input model
        if (model == null) {
            return null;
        }

        return TableInput.builder()
                .owner(model.getOwner())
                .viewOriginalText(model.getViewOriginalText())
                .description(model.getDescription())
                .tableType(model.getTableType())
                .parameters(translateToSDKParameters(model.getParameters()))
                .viewExpandedText(model.getViewExpandedText())
                .storageDescriptor(translateToSDKStorageDescriptor(model.getStorageDescriptor()))
                .targetTable(translateToSDKTableIdentifier(model.getTargetTable()))
                .partitionKeys(translateToSDKColumn(model.getPartitionKeys()))
                .retention(model.getRetention())
                .name(model.getName())
                .build();
    }

    /**
     * Translates the resource SkewedInfo model object to the corresponding
     * Glue SDK SkewedInfo object.
     *
     * @param model The resource model SkewedInfo object.
     * @return The corresponding Glue SDK SkewedInfo object.
     */
    static SkewedInfo translateToSDKSkewedInfo(final software.amazon.glue.table.SkewedInfo model) {
        if (model == null) {
            return null;
        }

        return SkewedInfo.builder()
                .skewedColumnNames(model.getSkewedColumnNames())
                .skewedColumnValues(model.getSkewedColumnValues())
                .skewedColumnValueLocationMaps(translateToSDKSkewedColumnValueLocationMaps(model.getSkewedColumnValueLocationMaps()))
                .build();
    }

    /**
     * Translates the resource SchemaId model object to the corresponding
     * Glue SDK SchemaId object.
     *
     * @param model The resource model SchemaId object.
     * @return The corresponding Glue SDK SchemaId object.
     */
    static SchemaId translateToSDKSchemaId(final software.amazon.glue.table.SchemaId model) {
        if (model == null) {
            return null;
        }

        return software.amazon.awssdk.services.glue.model.SchemaId.builder()
                .schemaArn(model.getSchemaArn())
                .schemaName(model.getSchemaName())
                .registryName(model.getRegistryName())
                .build();
    }

    /**
     * Translates the resource SchemaReference model object to the corresponding
     * Glue SDK SchemaReference object.
     *
     * @param model The resource model SchemaReference object.
     * @return The corresponding Glue SDK SchemaReference object.
     */
    static SchemaReference translateToSDKSchemaReference(final software.amazon.glue.table.SchemaReference model) {
        if (model == null) {
            return null;
        }

        SchemaReference.Builder builder = SchemaReference.builder()
                .schemaId(translateToSDKSchemaId(model.getSchemaId()))
                .schemaVersionId(model.getSchemaVersionId());

        Integer schemaVersionNumber = model.getSchemaVersionNumber();
        if (schemaVersionNumber != null) {
            builder.schemaVersionNumber(Long.valueOf(schemaVersionNumber));
        }
        return builder.build();
    }

    /**
     * Translates the resource StorageDescriptor model object to the corresponding
     * Glue SDK StorageDescriptor object.
     *
     * @param model The resource model StorageDescriptor object.
     * @return The corresponding Glue SDK StorageDescriptor object.
     */
    static StorageDescriptor translateToSDKStorageDescriptor(final software.amazon.glue.table.StorageDescriptor model) {
        if (model == null) {
            return null;
        }
        StorageDescriptor.Builder builder = StorageDescriptor.builder()
                .location(model.getLocation())
                .inputFormat(model.getInputFormat())
                .outputFormat(model.getOutputFormat())
                .compressed(model.getCompressed())
                .numberOfBuckets(model.getNumberOfBuckets())
                .serdeInfo(translateToSDKSerDeInfo(model.getSerdeInfo()))
                .bucketColumns(model.getBucketColumns())
                .sortColumns(translateToSDKOrder(model.getSortColumns()))
                .storedAsSubDirectories(model.getStoredAsSubDirectories())
                .parameters(translateToSDKParameters(model.getParameters()));

        List<software.amazon.glue.table.Column> columns = model.getColumns();
        software.amazon.glue.table.SchemaReference schemaReference = model.getSchemaReference();

        if (columns == null || columns.isEmpty()) {
            builder.schemaReference(translateToSDKSchemaReference(schemaReference));
        } else if (schemaReference == null) {
            builder.columns(translateToSDKColumn(columns));
        }
        builder.skewedInfo(translateToSDKSkewedInfo(model.getSkewedInfo()));
        return builder.build();
    }


    /**
     * Translates the resource SerDeInfo model object to the corresponding
     * Glue SDK SerdeInfo object.
     *
     * @param model The resource model SerdeInfo object.
     * @return The corresponding Glue SDK SerDeInfo object.
     */
    static SerDeInfo translateToSDKSerDeInfo(final software.amazon.glue.table.SerdeInfo model) {
        if (model == null) {
            return null;
        }

        return SerDeInfo.builder()
                .name(model.getName())
                .serializationLibrary(model.getSerializationLibrary())
                .parameters(translateToSDKParameters(model.getParameters()))
                .build();
    }

    // static OpenTableFormatInput translateOpenTableFormatInput(final software.amazon.glue.table.OpenTableFormatInput input) {
    //     if (input == null) {
    //         return null;
    //     }

    //     return OpenTableFormatInput.builder()
    //             .icebergInput(translateToIcebergInput(input.getIcebergInput()))
    //             .build();
    // }

    // static IcebergInput translateToIcebergInput(final software.amazon.glue.table.IcebergInput input) {
    //     if (input == null) {
    //         return null;
    //     }

    //     return IcebergInput.builder()
    //             .metadataOperation(input.getMetadataOperation())
    //             .version(input.getVersion())
    //             .build();
    // }


    public static OpenTableFormatInput translateToSdkOpenTableFormatInput(software.amazon.glue.table.OpenTableFormatInput input) {
        if (input == null) return null;
        return software.amazon.awssdk.services.glue.model.OpenTableFormatInput.builder()
            .icebergInput(translateToSdkIcebergInput(input.getIcebergInput()))
            .build();
    }

    public static IcebergInput translateToSdkIcebergInput(software.amazon.glue.table.IcebergInput input) {
        if (input == null) return null;
        return software.amazon.awssdk.services.glue.model.IcebergInput.builder()
            .metadataOperation(input.getMetadataOperation())
            .version(input.getVersion())
            .build();
    }
}
