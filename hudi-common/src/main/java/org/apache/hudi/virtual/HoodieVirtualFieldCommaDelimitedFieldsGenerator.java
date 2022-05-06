package org.apache.hudi.virtual;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.StringUtils;

public class HoodieVirtualFieldCommaDelimitedFieldsGenerator extends HoodieVirtualFieldGenerator {
  public HoodieVirtualFieldCommaDelimitedFieldsGenerator(List<String> requiredFieldsForGeneration) {
    super(requiredFieldsForGeneration);
  }

  @Override
  public String generateField(GenericRecord record) {
    return requiredFieldsForGeneration.stream().map(
        (String fieldName) -> (String) record.get(fieldName)
    ).collect(Collectors.joining(","));
  }

  @Override
  public boolean canGenerateField(GenericRecord record) {
    for (String field : requiredFieldsForGeneration) {
      if (record.get(field) == null) {
        return false;
      }
    }
    return true;
  }
}
