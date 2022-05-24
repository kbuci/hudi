package org.apache.hudi.virtual;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;

public final class HoodieVirtualKeyInfo {

  private final HoodieVirtualKeyConfig hoodieVirtualKeyConfig;
  private final boolean recordKeyVirtual;
  private final boolean partitionPathVirtual;
  private final Option<BaseKeyGenerator> hoodieKeyFieldsGenerator;

  public HoodieVirtualKeyInfo(HoodieVirtualKeyConfig hoodieVirtualKeyConfig){
    this.hoodieVirtualKeyConfig = hoodieVirtualKeyConfig;
    this.hoodieKeyFieldsGenerator = hoodieVirtualKeyConfig.getOptionalFieldGenerator(HoodieRecord.RECORD_KEY_METADATA_FIELD);

    // In order to avoid repeated String operations, store the virtual config for fields that are part of
    // HoodieKey (namely, hoodie record key and partition path) are virtual
    this.recordKeyVirtual = hoodieVirtualKeyConfig.isVirtualField(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    this.partitionPathVirtual = hoodieVirtualKeyConfig.isVirtualField(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
  }

  public boolean isRecordKeyVirtual() {
    return recordKeyVirtual;
  }

  public boolean isPartitionPathVirtual() {
    return partitionPathVirtual;
  }


  public String getRecordKey(GenericRecord record) {
    if (isRecordKeyVirtual()) {
      return computeVirtualField(hoodieKeyFieldsGenerator.get(), record);
    } else {
      return (String) record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    }
  }

  public String getPartitionPath(GenericRecord record) {
    if (isPartitionPathVirtual()) {
      return computeVirtualPartitionPathField(hoodieKeyFieldsGenerator.get(), record);
    } else {
      return (String) record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    }
  }

  public String getField(String field, GenericRecord record) {
    if (!hoodieVirtualKeyConfig.isVirtualField(field)){
      return (String) record.get(field);
    }
    Option<BaseKeyGenerator> generatorOption =  hoodieVirtualKeyConfig.getOptionalFieldGenerator(field);
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
    return computeVirtualField(generator, record);
  }

  public Iterable<String> getFields(Iterable<String> fields, GenericRecord record) {
    return StreamSupport.stream(fields.spliterator(), false).map((field) -> getField(field, record)).collect(
        Collectors.toList());
  }

  private static String computeVirtualField(KeyGenerator generator, GenericRecord record) {
    return generator.getKey(record).getRecordKey();
  }

  private static String computeVirtualPartitionPathField(KeyGenerator generator, GenericRecord record) {
    return generator.getKey(record).getPartitionPath();
  }


}
