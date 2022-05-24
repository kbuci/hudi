package org.apache.hudi.virtual;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
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
  private final Map<String, List<Integer>> virtualFieldToRequiredColumns;

  public HoodieVirtualKeyConfig(HoodieTableConfig config) {
    virtualFieldToOptionalGenerator = new HashMap();
    virtualFieldToRequiredColumns = new HashMap();

    List<String> allVirtualFields = config.virtualFields().isEmpty()? Collections.emptyList()
        : Collections.unmodifiableList(Arrays.stream(config.virtualFields().split(",")).collect(
            Collectors.toList()));
    try {
      for (String virtualField : allVirtualFields) {
        List requiredFields =  asIndexList(config.hoodieVirtualFieldsColumnsNeededForGeneratorsConfig(virtualField));
        virtualFieldToOptionalGenerator.put(virtualField, loadVirtualFieldGenerator(config.hoodieVirtualFieldsGeneratorsConfig(virtualField), config.getProps()));
        virtualFieldToRequiredColumns.put(virtualField, requiredFields);
      }
      Option<BaseKeyGenerator> hoodieKeyFieldsGenerator;
      if (config.getKeyGeneratorClassName() == null) {
        hoodieKeyFieldsGenerator = Option.empty();
      } else {
        hoodieKeyFieldsGenerator = loadVirtualFieldGenerator(config.getKeyGeneratorClassName(), config.getProps());
      }
      virtualFieldToOptionalGenerator.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, hoodieKeyFieldsGenerator);
      virtualFieldToOptionalGenerator.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, hoodieKeyFieldsGenerator);


    } catch (Throwable e) {
      throw new HoodieException("could not parse virtual field config " + e);
    }

  }

  public boolean isVirtualField(String field) {
    return virtualFieldToOptionalGenerator.containsKey(field);
  }

  public Option<BaseKeyGenerator> getOptionalFieldGenerator(String field) {
    return virtualFieldToOptionalGenerator.get(field);
  }

  public Iterable<Integer> getRequiredColumnsToGenerateField(String field) {
    return virtualFieldToRequiredColumns.get(field);
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
