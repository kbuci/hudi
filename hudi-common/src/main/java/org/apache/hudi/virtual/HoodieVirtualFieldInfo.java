package org.apache.hudi.virtual;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.KeyGenerator;
import org.codehaus.jackson.map.ObjectMapper;

public final class HoodieVirtualFieldInfo {

  private final List<String> allVirtualFields;
  private final Map<String, HoodieVirtualFieldGeneratorInterface> virtualFieldToGenerator;
  private final boolean recordKeyVirtual;
  private final boolean partitionPathVirtual;
  private final Option<HoodieVirtualFieldGeneratorInterface> recordKeyGenerator;
  private final Option<HoodieVirtualFieldGeneratorInterface> partitionPathGenerator;

  public HoodieVirtualFieldInfo(HoodieTableConfig config){
    allVirtualFields = config.virtualFields().isEmpty()? Collections.singletonList("")
        : Collections.unmodifiableList(Arrays.stream(config.virtualFields().split(",")).sorted().collect(
        Collectors.toList()));

    virtualFieldToGenerator = new HashMap();

    try {
      final Map<String, List<String>> virtualFieldToRequiredFields = (new ObjectMapper()).readValue(config.virtualFieldsGenerators(), Map.class);
      Map<String, String> virtualFieldToGeneratorName = (new ObjectMapper()).readValue(config.virtualFieldsGenerators(), Map.class);
      for (Map.Entry<String,String> entry : virtualFieldToGeneratorName.entrySet()) {
        List requiredFields = virtualFieldToRequiredFields.get(entry.getKey());
        HoodieVirtualFieldGeneratorInterface generator = loadVirtualFieldGenerator(entry.getValue(), requiredFields).get();
        virtualFieldToGenerator.put(entry.getKey(), generator);
      }


    } catch (Throwable e) {
      throw new HoodieException("could not parse virtual field config ");
    }

    boolean recordKeyVirtual = false;
    boolean partitionPathVirtual = false;

    // For metadata fields, ideally I would use exeisting BaseKeyGenerator objects.
    // The reason I had these use the new HoodieVirutalFieldGenerator objects was because
    // - As I understand it , org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_CLASS_NAME cam
    // be set from write config, but we want to make sure this property cannot be changed by hoodie
    // user once it's in the properties file
    // - The BaseKeyGenerator is responsible for generating both the record key and partition path.
    // but with virtual fields I wasn't sure how to alter it to keep them both independently configurable
    try {
      if (allVirtualFields.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
        recordKeyVirtual = true;
        List requiredFields = Arrays.asList(
            config.hoodieRecordKeyVirtualFieldRequiredColumns().split(","));
        HoodieVirtualFieldGeneratorInterface generator = loadVirtualFieldGenerator(config.hoodieRecordKeyVirtualFieldGenerator(), requiredFields).get();
        recordKeyGenerator = Option.of(generator);
      } else {
        recordKeyGenerator = Option.empty();
      }

      if (allVirtualFields.contains(HoodieRecord.PARTITION_PATH_METADATA_FIELD)) {
        recordKeyVirtual = true;
        List requiredFields = Arrays.asList(
            config.hoodiePartitionPathVirtualFieldRequiredColumns().split(","));
        HoodieVirtualFieldGeneratorInterface generator = loadVirtualFieldGenerator(config.hoodiePartitionPathVirtualFieldGenerator(), requiredFields).get();
        partitionPathGenerator = Option.of(generator);
      } else {
        partitionPathGenerator = Option.empty();
      }
    } catch (Exception e) {
      throw new HoodieException("could not parse virtual meta field config ");
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
    if (recordKeyGenerator.isPresent()) {
      return computeField(recordKeyGenerator.get(), record);
    }
    throw new HoodieException("No virtual field generator present for record key");
  }

  public String getPartitionPath(GenericRecord record) {
    if (partitionPathGenerator.isPresent()) {
      return computeField(partitionPathGenerator.get(), record);
    }
    throw new HoodieException("No virtual field generator present for record key");
  }

  public String getField(String field, GenericRecord record) {
    if (field.equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)){
      return getRecordKey(record);
    }
    if (field.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)){
      return getPartitionPath(record);
    }
    if (!allVirtualFields.contains(field)){
      return (String) record.get(field);
    }
    return computeField(virtualFieldToGenerator.get(field), record);
  }

  private static String computeField(final HoodieVirtualFieldGeneratorInterface generator, GenericRecord record) {
    if (generator.canGenerateField(record)) {
      return generator.generateField(record);
    }
    throw new HoodieException("Could not use generator " + generator + " on record " + record.toString());
  }

  private static Option<HoodieVirtualFieldGenerator> loadVirtualFieldGenerator(String generatorClass, List requiredFields) {
    if (generatorClass.isEmpty()){
      return Option.empty();
    }
    try {
      return  Option.of((HoodieVirtualFieldGenerator) ReflectionUtils.loadClass(generatorClass, requiredFields));
    } catch (Throwable e) {
      throw new HoodieIOException("Could not load virtual field generator class " + generatorClass, e);
    }
  }



}
