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
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Variant-related conversion in {@link RowDataToAvroConverters}.
 *
 * <p>Since this module compiles against Flink 1.20 (pre-2.1), native VariantType
 * is not available. Tests that require native Variant (e.g. createVariantConverter)
 * cannot run here. Instead we verify:
 * <ul>
 *   <li>ROW-based variant via the row converter still produces correct Avro records</li>
 *   <li>Shredded variant throws as expected</li>
 *   <li>Nested variant in arrays/maps works through the row converter path</li>
 * </ul>
 */
public class TestRowDataToAvroConvertersVariant {

  @Test
  public void testShreddedVariantWriteThrows() {
    RowType variantRowType = RowType.of(
        new VarBinaryType(VarBinaryType.MAX_LENGTH),
        new VarBinaryType(VarBinaryType.MAX_LENGTH));

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(variantRowType);

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

  @Test
  public void testVariantInArrayWriteSucceeds() {
    // ARRAY<ROW<metadata BYTES, value BYTES>> representing ARRAY<VARIANT>
    RowType variantRowType = RowType.of(
        new VarBinaryType(VarBinaryType.MAX_LENGTH),
        new VarBinaryType(VarBinaryType.MAX_LENGTH));
    ArrayType arrayType = new ArrayType(variantRowType);
    RowType outerType = RowType.of(arrayType);

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(outerType);

    HoodieSchema variantSchema = HoodieSchema.createVariant();
    HoodieSchema arraySchema = HoodieSchema.createArray(variantSchema);
    HoodieSchema recordSchema = HoodieSchema.createRecord("test", null, null,
        Arrays.asList(HoodieSchemaField.of("variants", arraySchema)));

    GenericRowData v1 = GenericRowData.of(new byte[] {0x01}, new byte[] {0x10});
    GenericRowData v2 = GenericRowData.of(new byte[] {0x01}, new byte[] {0x20});
    GenericArrayData arrayData = new GenericArrayData(new GenericRowData[] {v1, v2});
    GenericRowData outerRow = GenericRowData.of(arrayData);

    Object result = converter.convert(recordSchema, outerRow);
    assertNotNull(result);
    assertTrue(result instanceof GenericRecord);
    GenericRecord record = (GenericRecord) result;
    Object arrayResult = record.get("variants");
    assertNotNull(arrayResult);
    assertTrue(arrayResult instanceof List);
    assertEquals(2, ((List<?>) arrayResult).size());
    assertTrue(((List<?>) arrayResult).get(0) instanceof GenericRecord);
  }

  @Test
  public void testVariantInMapValueWriteSucceeds() {
    // MAP<STRING, ROW<metadata BYTES, value BYTES>> representing MAP<STRING, VARIANT>
    RowType variantRowType = RowType.of(
        new VarBinaryType(VarBinaryType.MAX_LENGTH),
        new VarBinaryType(VarBinaryType.MAX_LENGTH));
    MapType mapType = new MapType(new VarCharType(VarCharType.MAX_LENGTH), variantRowType);
    RowType outerType = RowType.of(mapType);

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(outerType);

    HoodieSchema variantSchema = HoodieSchema.createVariant();
    HoodieSchema mapSchema = HoodieSchema.createMap(variantSchema);
    HoodieSchema recordSchema = HoodieSchema.createRecord("test", null, null,
        Arrays.asList(HoodieSchemaField.of("variant_map", mapSchema)));

    GenericRowData v1 = GenericRowData.of(new byte[] {0x01}, new byte[] {0x10});
    Map<StringData, GenericRowData> mapData = new HashMap<>();
    mapData.put(StringData.fromString("key1"), v1);
    GenericMapData genericMapData = new GenericMapData(mapData);
    GenericRowData outerRow = GenericRowData.of(genericMapData);

    Object result = converter.convert(recordSchema, outerRow);
    assertNotNull(result);
    assertTrue(result instanceof GenericRecord);
    GenericRecord record = (GenericRecord) result;
    Object mapResult = record.get("variant_map");
    assertNotNull(mapResult);
    assertTrue(mapResult instanceof Map);
    assertEquals(1, ((Map<?, ?>) mapResult).size());
    assertTrue(((Map<?, ?>) mapResult).values().iterator().next() instanceof GenericRecord);
  }
}
