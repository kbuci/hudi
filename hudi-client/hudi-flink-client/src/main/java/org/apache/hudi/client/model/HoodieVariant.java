/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.model;

import org.apache.flink.types.variant.Variant;

import java.util.Arrays;

/**
 * Concrete implementation of Flink's {@link Variant} interface that holds
 * the raw metadata and value byte arrays from Hudi's internal Variant representation
 * ({@code ROW<metadata BYTES, value BYTES>}).
 *
 * <p>This bridges Hudi's ROW-based Variant storage with Flink's Variant API,
 * allowing {@code RowData.getVariant()} to return a concrete object instead
 * of throwing {@code UnsupportedOperationException}.</p>
 */
public class HoodieVariant implements Variant {

  private static final long serialVersionUID = 1L;

  private final byte[] metadata;
  private final byte[] value;

  public HoodieVariant(byte[] metadata, byte[] value) {
    this.metadata = metadata;
    this.value = value;
  }

  public byte[] getMetadata() {
    return metadata;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HoodieVariant)) {
      return false;
    }
    HoodieVariant that = (HoodieVariant) o;
    return Arrays.equals(metadata, that.metadata) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(metadata);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "HoodieVariant{metadata=" + (metadata != null ? metadata.length + " bytes" : "null")
        + ", value=" + (value != null ? value.length + " bytes" : "null") + "}";
  }

  /**
   * Extracts a HoodieVariant from a RowData that represents a Variant
   * as {@code ROW<metadata BYTES, value BYTES>}.
   */
  public static HoodieVariant fromRowData(org.apache.flink.table.data.RowData variantRow) {
    byte[] metadata = variantRow.isNullAt(0) ? null : variantRow.getBinary(0);
    byte[] value = variantRow.isNullAt(1) ? null : variantRow.getBinary(1);
    return new HoodieVariant(metadata, value);
  }
}
