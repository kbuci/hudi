package org.apache.hudi.virtual;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

public final class HoodieVirtualFieldInfo {

  private final List<String> allVirtualFields;
  private final Map<String, Option<BaseKeyGenerator>> virtualFieldToOptionalGenerator;
  private final Map<String, List> virtualFieldToRequiredColumns;
  private final boolean recordKeyVirtual;
  private final boolean partitionPathVirtual;
  private final List<Integer> recordKeyRequiredColumns;
  private final List<Integer> partitionPathRequiredColumns;
  private final Option<KeyGenerator> hoddieKeyFieldsGenerator;

  public HoodieVirtualFieldInfo(HoodieTableConfig config, KeyGeneratorOptions keyGeneratorOptions){
    allVirtualFields = config.virtualFields().isEmpty()? Collections.singletonList("")
        : Collections.unmodifiableList(Arrays.stream(config.virtualFields().split(",")).sorted().collect(
        Collectors.toList()));

    virtualFieldToOptionalGenerator = new HashMap();
    virtualFieldToRequiredColumns = new HashMap<>();

    try {
      for (String virtualField : allVirtualFields) {
        List requiredFields =  asIndexList(config.hoodieVirtualFieldsColumnsNeededForGeneratorsConfig(virtualField));
        virtualFieldToOptionalGenerator.put(virtualField, loadVirtualFieldGenerator(config.hoodieVirtualFieldsGeneratorsConfig(virtualField), config.getProps()));
        virtualFieldToRequiredColumns.put(virtualField, requiredFields);
      }


    } catch (Throwable e) {
      throw new HoodieException("could not parse virtual field config " + e);
    }

    if (config.getKeyGeneratorClassName() == null) {
      hoddieKeyFieldsGenerator = Option.empty();
    } else {
      hoddieKeyFieldsGenerator = loadVirtualFieldGenerator(config.getKeyGeneratorClassName(), config.getProps());
    }

    recordKeyRequiredColumns = virtualFieldToRequiredColumns.get(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    partitionPathRequiredColumns = virtualFieldToRequiredColumns.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD);

    boolean recordKeyVirtual = false;
    boolean partitionPathVirtual = false;

    if (allVirtualFields.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
      recordKeyVirtual = true;
    }

    if (allVirtualFields.contains(HoodieRecord.PARTITION_PATH_METADATA_FIELD)) {
      partitionPathVirtual = true;
    }

    this.recordKeyVirtual = recordKeyVirtual;
    this.partitionPathVirtual = partitionPathVirtual;

  }

  public boolean isRecordKeyVirtual() {
    return recordKeyVirtual;
  }

  public boolean isPartitionPathVirtual() {
    return partitionPathVirtual;
  }


  public String getRecordKey(GenericRecord record) {
    if (isRecordKeyVirtual()) {

    } else {
      return record.get()
    }
  }

  public String getPartitionPath(GenericRecord record) {
    return getHoodkeKeyFieldsFromRecord(record).getPartitionPath();
  }

  public String getField(String field, GenericRecord record) {
    if (!allVirtualFields.contains(field)){
      return (String) record.get(field);
    }
    Option<BaseKeyGenerator> generatorOption =  virtualFieldToOptionalGenerator.get(field);
    if (!generatorOption.isPresent()) {
      throw new HoodieException("No generator present");
    }
    BaseKeyGenerator generator = generatorOption.get();
    if (field.equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)){
      return generator.getRecordKey(record);
    }
    if (field.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)){
      return generator.getPartitionPath(record);
    }

    return computeField(generator, virtualFieldToRequiredColumns.get(field), record);
  }

  public String getFields(List<String> field, GenericRecord record) {
    if (field.equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)){
      return getRecordKey(record);
    }
    if (field.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)){
      return getPartitionPath(record);
    }
    if (!allVirtualFields.contains(field)){
      return (String) record.get(field);
    }
    if (!virtualFieldToOptionalGenerator.get(field).isPresent()) {
      throw new HoodieException("No generator present");
    }
    return computeField(virtualFieldToOptionalGenerator.get(field).get(), virtualFieldToRequiredColumns.get(field), record);
  }

  private static String computeField(KeyGenerator generator, List<Integer> requiredColumns, GenericRecord record) {
    for (Integer column : requiredColumns) {
      if (record.get(column.intValue()) == null){
        throw new HoodieException("Could not use generator " + generator + " on record " + record.toString());
      }
    }
    return generator.getKey(record).getRecordKey();
  }

  private static Option<KeyGenerator> loadVirtualFieldGenerator(String generatorClass, TypedProperties props) {
    if (generatorClass.isEmpty()){
      return Option.empty();
    }
    try {
      return  Option.of((BaseKeyGenerator) ReflectionUtils.loadClass(generatorClass, props));
    } catch (Throwable e) {
      throw new HoodieIOException("Could not load virtual field generator class " +  e);
    }
  }

  private static List<Integer> asIndexList(String fieldNames) {
    return Arrays.stream(fieldNames.split(",")).map((field) -> Integer.parseInt(field)).collect(Collectors.toList());
  }


}
