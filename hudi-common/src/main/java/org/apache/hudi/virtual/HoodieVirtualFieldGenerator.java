package org.apache.hudi.virtual;

import java.util.List;

public abstract class HoodieVirtualFieldGenerator implements HoodieVirtualFieldGeneratorInterface{
  protected final List<String> requiredFieldsForGeneration;

  protected HoodieVirtualFieldGenerator(List<String> requiredFieldsForGeneration) {
    this.requiredFieldsForGeneration = requiredFieldsForGeneration;
  }
}
