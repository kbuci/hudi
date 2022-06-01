package org.apache.hudi.virtual;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;

public class HoodieVirtualKeyConfig {

  private final Map<String, Option<BaseKeyGenerator>> virtualFieldToOptionalGenerator;
  private final List<Option<BaseKeyGenerator>> virtualFieldIndexToOptionalGenerator;
  private final List<Boolean> isVirtualFieldIndex;
  private final Map<String, List<Integer>> virtualFieldToRequiredColumns;
  private final List<Integer> virtualFieldIndices;

  public HoodieVirtualKeyConfig(HoodieTableConfig config, Schema schema) {
    virtualFieldToOptionalGenerator = new HashMap();
    final int numSchemaFields = schema.getFields().size();
    virtualFieldIndexToOptionalGenerator = Arrays.asList(new Option[numSchemaFields]);
    virtualFieldToRequiredColumns = new HashMap();

    final List<String> allVirtualFields = config.virtualFields().isEmpty()? Collections.emptyList()
        : Collections.unmodifiableList(Arrays.stream(config.virtualFields().split(",")).collect(
            Collectors.toList()));

    virtualFieldIndices = new ArrayList<>();
    for (String field : allVirtualFields) {
      int index = schema.getField(field).pos();
      virtualFieldIndices.add(index);
    }

    isVirtualFieldIndex = new ArrayList<>();
    for (int index = 0; index < numSchemaFields; index++) {
      isVirtualFieldIndex.add(virtualFieldIndices.contains(index));
    }

//    try {
      for (String virtualField : allVirtualFields) {
        List requiredFields =  asIndexList(config.hoodieVirtualFieldsColumnsNeededForGeneratorsConfig(virtualField));
        Option<BaseKeyGenerator> fieldGenerator = loadVirtualFieldGenerator(config.hoodieVirtualFieldsGeneratorsConfig(virtualField), config.getProps());
        virtualFieldToOptionalGenerator.put(virtualField, fieldGenerator);
        virtualFieldIndexToOptionalGenerator.set(schema.getField(virtualField).pos(), fieldGenerator);
        virtualFieldToRequiredColumns.put(virtualField, requiredFields);
      }
      Option<BaseKeyGenerator> hoodieKeyFieldsGenerator;
      if (config.getKeyGeneratorClassName() == null) {
        hoodieKeyFieldsGenerator = Option.empty();
      } else {
        hoodieKeyFieldsGenerator = loadVirtualFieldGenerator(config.getKeyGeneratorClassName(), config.getProps());
      }
      virtualFieldToOptionalGenerator.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, hoodieKeyFieldsGenerator);
      virtualFieldIndexToOptionalGenerator.set(schema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD).pos(), hoodieKeyFieldsGenerator);
      virtualFieldToOptionalGenerator.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, hoodieKeyFieldsGenerator);
      virtualFieldIndexToOptionalGenerator.set(schema.getField(HoodieRecord.PARTITION_PATH_METADATA_FIELD).pos(), hoodieKeyFieldsGenerator);


//    } catch (Throwable e) {
//      throw new HoodieException("could not parse virtual field config " + e);
//    }

  }

  public boolean isVirtualField(String field) {
    return virtualFieldToOptionalGenerator.containsKey(field);
  }

  public boolean isVirtualField(int fieldPos) {
    return isVirtualFieldIndex.get(fieldPos);
  }

  public Option<BaseKeyGenerator> getOptionalFieldGenerator(String field) {
    return virtualFieldToOptionalGenerator.get(field);
  }

  public Option<BaseKeyGenerator> getOptionalFieldGenerator(int field) {
    return virtualFieldIndexToOptionalGenerator.get(field);
  }

  public Iterable<Integer> getRequiredColumnsToGenerateField(String field) {
    return virtualFieldToRequiredColumns.get(field);
  }

  public Iterable<Integer> getVirtualFieldIndices() {
    return virtualFieldIndices;
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
