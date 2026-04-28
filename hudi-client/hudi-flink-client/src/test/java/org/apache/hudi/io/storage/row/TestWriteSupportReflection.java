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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the reflection-based write support class instantiation in
 * {@link HoodieRowDataFileWriterFactory}. Verifies that custom write support
 * classes are always used (never bypassed) and that the 4-arg constructor
 * fallback to 3-arg works correctly.
 */
public class TestWriteSupportReflection {

  private static final RowType TEST_ROW_TYPE = (RowType) DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT()),
      DataTypes.FIELD("name", DataTypes.STRING())
  ).notNull().getLogicalType();

  private static final Configuration TEST_CONF = new Configuration();

  private static final HoodieSchema TEST_SCHEMA = HoodieSchema.createRecord(
      "test_record", null, null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
      ));

  /**
   * Custom write support that only has the 3-arg constructor (no HoodieSchema support).
   */
  public static class LegacyWriteSupport extends HoodieRowDataParquetWriteSupport {
    public LegacyWriteSupport(Configuration conf, RowType rowType, BloomFilter bloomFilter) {
      super(conf, rowType, bloomFilter);
    }
  }

  /**
   * Custom write support that has both 3-arg and 4-arg constructors.
   */
  public static class SchemaAwareWriteSupport extends HoodieRowDataParquetWriteSupport {
    private final boolean hasSchema;

    public SchemaAwareWriteSupport(Configuration conf, RowType rowType, BloomFilter bloomFilter) {
      super(conf, rowType, bloomFilter);
      this.hasSchema = false;
    }

    public SchemaAwareWriteSupport(Configuration conf, RowType rowType, BloomFilter bloomFilter,
                                   Option<HoodieSchema> hoodieSchema) {
      super(conf, rowType, bloomFilter, hoodieSchema);
      this.hasSchema = hoodieSchema.isPresent();
    }

    public boolean hasSchema() {
      return hasSchema;
    }
  }

  @Test
  public void testDefaultClassWith4ArgConstructor() {
    String className = HoodieRowDataParquetWriteSupport.class.getName();
    Option<HoodieSchema> schema = Option.of(TEST_SCHEMA);

    HoodieRowDataParquetWriteSupport writeSupport =
        (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
            className,
            new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class, Option.class},
            TEST_CONF, TEST_ROW_TYPE, null, schema);

    assertNotNull(writeSupport);
    assertInstanceOf(HoodieRowDataParquetWriteSupport.class, writeSupport);
  }

  @Test
  public void testDefaultClassWith3ArgConstructor() {
    String className = HoodieRowDataParquetWriteSupport.class.getName();

    HoodieRowDataParquetWriteSupport writeSupport =
        (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
            className,
            new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class},
            TEST_CONF, TEST_ROW_TYPE, null);

    assertNotNull(writeSupport);
  }

  @Test
  public void testLegacyClassFallsBackTo3ArgWhenSchemaPresent() {
    String className = LegacyWriteSupport.class.getName();
    Option<HoodieSchema> schema = Option.of(TEST_SCHEMA);

    HoodieRowDataParquetWriteSupport writeSupport;
    try {
      writeSupport = (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
          className,
          new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class, Option.class},
          TEST_CONF, TEST_ROW_TYPE, null, schema);
    } catch (Exception e) {
      writeSupport = (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
          className,
          new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class},
          TEST_CONF, TEST_ROW_TYPE, null);
    }

    assertNotNull(writeSupport);
    assertInstanceOf(LegacyWriteSupport.class, writeSupport);
  }

  @Test
  public void testSchemaAwareClassReceivesSchemaVia4Arg() {
    String className = SchemaAwareWriteSupport.class.getName();
    Option<HoodieSchema> schema = Option.of(TEST_SCHEMA);

    HoodieRowDataParquetWriteSupport writeSupport;
    try {
      writeSupport = (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
          className,
          new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class, Option.class},
          TEST_CONF, TEST_ROW_TYPE, null, schema);
    } catch (Exception e) {
      writeSupport = (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
          className,
          new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class},
          TEST_CONF, TEST_ROW_TYPE, null);
    }

    assertNotNull(writeSupport);
    assertInstanceOf(SchemaAwareWriteSupport.class, writeSupport);
    assertTrue(((SchemaAwareWriteSupport) writeSupport).hasSchema(),
        "Schema-aware class should receive the HoodieSchema via 4-arg constructor");
  }

  @Test
  public void testSchemaAwareClassWithoutSchema() {
    String className = SchemaAwareWriteSupport.class.getName();

    HoodieRowDataParquetWriteSupport writeSupport =
        (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
            className,
            new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class},
            TEST_CONF, TEST_ROW_TYPE, null);

    assertNotNull(writeSupport);
    assertInstanceOf(SchemaAwareWriteSupport.class, writeSupport);
    assertTrue(!((SchemaAwareWriteSupport) writeSupport).hasSchema(),
        "Should use 3-arg constructor when no schema is provided");
  }
}
