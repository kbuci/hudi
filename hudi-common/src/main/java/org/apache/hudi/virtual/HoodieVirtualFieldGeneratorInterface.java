package org.apache.hudi.virtual;

import org.apache.avro.generic.GenericRecord;

public interface HoodieVirtualFieldGeneratorInterface {
  public String generateField(GenericRecord record);

  public boolean canGenerateField(GenericRecord record);
}
