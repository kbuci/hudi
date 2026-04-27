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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Variant-related conversion in {@link RowDataToAvroConverters}.
 */
public class TestRowDataToAvroConvertersVariant {

  @Test
  public void testShreddedVariantWriteThrows() {
    // Build a ROW type matching the Flink representation of a variant (2 binary fields)
    RowType variantRowType = RowType.of(
        new VarBinaryType(VarBinaryType.MAX_LENGTH),
        new VarBinaryType(VarBinaryType.MAX_LENGTH));

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(variantRowType);

    // Create a shredded variant schema
    HoodieSchema.Variant shreddedSchema = HoodieSchema.createVariantShredded(
        HoodieSchema.create(HoodieSchemaType.INT));

    GenericRowData row = GenericRowData.of(
        new byte[] {0x01}, new byte[] {0x02});

    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> converter.convert(shreddedSchema, row));
    assertTrue(ex.getMessage().contains("Shredded Variant is not yet supported in Flink"));
  }

  @Test
  public void testUnshreddedVariantWriteSucceeds() {
    RowType variantRowType = RowType.of(
        new VarBinaryType(VarBinaryType.MAX_LENGTH),
        new VarBinaryType(VarBinaryType.MAX_LENGTH));

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(variantRowType);

    HoodieSchema.Variant unshreddedSchema = HoodieSchema.createVariant();

    byte[] metadata = new byte[] {0x01, 0x00};
    byte[] value = new byte[] {0x02, 0x05};
    GenericRowData row = GenericRowData.of(metadata, value);

    Object result = converter.convert(unshreddedSchema, row);
    assertNotNull(result);
    assertTrue(result instanceof GenericRecord);
    GenericRecord record = (GenericRecord) result;
    assertNotNull(record.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD));
    assertNotNull(record.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD));
  }
}
