package org.apache.hudi.virtual;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;

public final class HoodieVirtualKeyInfo {

  private final List<String> allVirtualFields;
  private final Map<String, Option<BaseKeyGenerator>> virtualFieldToOptionalGenerator;
  private final Map<String, List<Integer>> virtualFieldToRequiredColumns;
  private final boolean recordKeyVirtual;
  private final boolean partitionPathVirtual;
  private final List<Integer> recordKeyRequiredColumns;
  private final List<Integer> partitionPathRequiredColumns;
  private final Option<BaseKeyGenerator> hoddieKeyFieldsGenerator;

  public HoodieVirtualKeyInfo(HoodieTableConfig config){
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
      virtualFieldToOptionalGenerator.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, hoddieKeyFieldsGenerator);
      virtualFieldToOptionalGenerator.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, hoddieKeyFieldsGenerator);
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
      return computeField(hoddieKeyFieldsGenerator.get(), recordKeyRequiredColumns, record);
    } else {
      return (String) record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    }
  }

  public String getPartitionPath(GenericRecord record) {
    if (isPartitionPathVirtual()) {
      return computePartitionPathField(hoddieKeyFieldsGenerator.get(), partitionPathRequiredColumns, record);
    } else {
      return (String) record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    }
  }

  private String getFieldWithRequiredColumns(String field, GenericRecord record, Iterable<Integer> requiredColumns) {
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
    return computeField(generator, requiredColumns, record);
  }

  public String getField(String field, GenericRecord record) {
    if (!allVirtualFields.contains(field)){
      return (String) record.get(field);
    }
    return getFieldWithRequiredColumns(field, record,  virtualFieldToRequiredColumns.get(field));
  }

  public Iterable<String> getFields(Iterable<String> fields, GenericRecord record) {
    Set allRequiredColumns = new TreeSet<>();
    List<String> fieldValues = null;
    for (String field : fields ) {
      if (!allVirtualFields.contains(field)){
        fieldValues.add( (String) record.get(field));
        continue;
      }
      for (Integer requiredIndex : virtualFieldToRequiredColumns.get(field)) {
       allRequiredColumns.add(requiredIndex);
      }
      fieldValues.add(getFieldWithRequiredColumns(field, record, allRequiredColumns));
    }
    return fieldValues;
  }

  private static String assertCanComputeField(KeyGenerator generator, Iterable<Integer> requiredColumns, GenericRecord record) {
    for (Integer column : requiredColumns) {
      if (record.get(column.intValue()) == null){
        throw new HoodieException("Could not use generator " + generator + " on record " + record.toString());
      }
    }
    return generator.getKey(record).getRecordKey();
  }

  private static String computeField(KeyGenerator generator, Iterable<Integer> requiredColumns, GenericRecord record) {
    assertCanComputeField(generator, requiredColumns, record);
    return generator.getKey(record).getRecordKey();
  }

  private static String computePartitionPathField(KeyGenerator generator, Iterable<Integer> requiredColumns, GenericRecord record) {
    assertCanComputeField(generator, requiredColumns, record);
    return generator.getKey(record).getPartitionPath();
  }

  private static Option<BaseKeyGenerator> loadVirtualFieldGenerator(String generatorClass, TypedProperties props) {
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
    if (fieldNames.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.stream(fieldNames.split(",")).map((field) -> Integer.parseInt(field)).collect(Collectors.toList());
  }

}
