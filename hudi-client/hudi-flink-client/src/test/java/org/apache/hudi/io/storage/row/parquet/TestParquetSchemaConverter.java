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

package org.apache.hudi.io.storage.row.parquet;

import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ParquetSchemaConverter}.
 */
public class TestParquetSchemaConverter {
  private static final RowType ROW_TYPE =
      RowType.of(
          new VarCharType(VarCharType.MAX_LENGTH),
          new BooleanType(),
          new TinyIntType(),
          new SmallIntType(),
          new IntType(),
          new BigIntType(),
          new FloatType(),
          new DoubleType(),
          new TimestampType(6),
          new DecimalType(5, 0),
          new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
          new ArrayType(new BooleanType()),
          new ArrayType(new TinyIntType()),
          new ArrayType(new SmallIntType()),
          new ArrayType(new IntType()),
          new ArrayType(new BigIntType()),
          new ArrayType(new FloatType()),
          new ArrayType(new DoubleType()),
          new ArrayType(new TimestampType(6)),
          new ArrayType(new DecimalType(5, 0)),
          new MapType(
              new VarCharType(VarCharType.MAX_LENGTH),
              new VarCharType(VarCharType.MAX_LENGTH)),
          new MapType(new IntType(), new BooleanType()),
          RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()));

  private static final RowType NESTED_ARRAY_MAP_TYPE =
      RowType.of(
          new IntType(),
          new ArrayType(true, new IntType()),
          new ArrayType(true, new ArrayType(true, new IntType())),
          new ArrayType(
              true,
              new MapType(
                  true,
                  new VarCharType(VarCharType.MAX_LENGTH),
                  new VarCharType(VarCharType.MAX_LENGTH))),
          new ArrayType(
              true,
              new RowType(
                  Collections.singletonList(
                      new RowType.RowField("a", new IntType())))),
          RowType.of(
              new IntType(),
              new ArrayType(
                  true,
                  new RowType(
                      Arrays.asList(
                          new RowType.RowField(
                              "b",
                              new ArrayType(
                                  true,
                                  new ArrayType(
                                      true, new IntType()))),
                          new RowType.RowField("c", new IntType()))))));

  @Test
  void testParquetFlinkTypeConverting() {
    MessageType messageType = ParquetSchemaConverter.convertToParquetMessageType("flink_schema", ROW_TYPE);
    RowType rowType = ParquetSchemaConverter.convertToRowType(messageType);
    assertThat(rowType, is(ROW_TYPE));

    messageType = ParquetSchemaConverter.convertToParquetMessageType("flink_schema", NESTED_ARRAY_MAP_TYPE);
    rowType = ParquetSchemaConverter.convertToRowType(messageType);
    assertThat(rowType, is(NESTED_ARRAY_MAP_TYPE));
  }

  @Test
  void testConvertComplexTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("f_array",
            DataTypes.ARRAY(DataTypes.CHAR(10).notNull())),
        DataTypes.FIELD("f_map",
            DataTypes.MAP(DataTypes.INT(), DataTypes.VARCHAR(20))),
        DataTypes.FIELD("f_row",
            DataTypes.ROW(
                DataTypes.FIELD("f_row_f0", DataTypes.INT()),
                DataTypes.FIELD("f_row_f1", DataTypes.VARCHAR(10)),
                DataTypes.FIELD("f_row_f2",
                    DataTypes.ROW(
                        DataTypes.FIELD("f_row_f2_f0", DataTypes.INT()),
                        DataTypes.FIELD("f_row_f2_f1", DataTypes.VARCHAR(10)))))));
    org.apache.parquet.schema.MessageType messageType =
        ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());
    assertThat(messageType.getColumns().size(), is(7));
    final String expected = "message converted {\n"
        + "  optional group f_array (LIST) {\n"
        + "    repeated group list {\n"
        + "      required binary element (STRING);\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required int32 key;\n"
        + "      optional binary value (STRING);\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_row {\n"
        + "    optional int32 f_row_f0;\n"
        + "    optional binary f_row_f1 (STRING);\n"
        + "    optional group f_row_f2 {\n"
        + "      optional int32 f_row_f2_f0;\n"
        + "      optional binary f_row_f2_f1 (STRING);\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }

  @Test
  void testConvertNestedComplexTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("f_array",
            DataTypes.ARRAY(DataTypes.ROW(
                DataTypes.FIELD("f_array_f0", DataTypes.INT()),
                DataTypes.FIELD("f_array_f1", DataTypes.VARCHAR(10).notNull()),
                DataTypes.FIELD("f_array_f3", DataTypes.ARRAY(DataTypes.CHAR(10).notNull()))).notNull())),
        DataTypes.FIELD("f_map",
            DataTypes.MAP(DataTypes.INT(), DataTypes.ROW(
                DataTypes.FIELD("f_map_f0", DataTypes.INT()),
                DataTypes.FIELD("f_map_f1", DataTypes.VARCHAR(10))).notNull())));

    org.apache.parquet.schema.MessageType messageType = ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());

    assertThat(messageType.getColumns().size(), is(6));
    final String expected = "message converted {\n"
        + "  optional group f_array (LIST) {\n"
        + "    repeated group list {\n"
        + "      required group element {\n"
        + "        optional int32 f_array_f0;\n"
        + "        required binary f_array_f1 (STRING);\n"
        + "        optional group f_array_f3 (LIST) {\n"
        + "          repeated group list {\n"
        + "            required binary element (STRING);\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "  optional group f_map (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required int32 key;\n"
        + "      required group value {\n"
        + "        optional int32 f_map_f0;\n"
        + "        optional binary f_map_f1 (STRING);\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }

  @Test
  void testConvertTimestampTypes() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("ts_3", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("ts_6", DataTypes.TIMESTAMP(6)),
        DataTypes.FIELD("ts_9", DataTypes.TIMESTAMP(9)));
    org.apache.parquet.schema.MessageType messageType =
        ParquetSchemaConverter.convertToParquetMessageType("converted", (RowType) dataType.getLogicalType());
    assertThat(messageType.getColumns().size(), is(3));
    final String expected = "message converted {\n"
        + "  optional int64 ts_3 (TIMESTAMP(MILLIS,true));\n"
        + "  optional int64 ts_6 (TIMESTAMP(MICROS,true));\n"
        + "  optional int96 ts_9;\n"
        + "}\n";
    assertThat(messageType.toString(), is(expected));
  }

  /**
   * Structural fallback: Parquet group with metadata + value binary fields (no VARIANT annotation).
   * Covers files written by Spark 4.0.x which does not annotate variant groups.
   */
  @Test
  void testVariantParquetReadStructuralFallback() {
    MessageType variantParquet = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
            Type.Repetition.REQUIRED).named("id"),
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("metadata"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("value"))
            .named("data"));

    if (HoodieSchemaConverter.tryCreateVariantDataType() != null) {
      RowType rowType = ParquetSchemaConverter.convertToRowType(variantParquet);
      assertEquals(2, rowType.getFieldCount());
      assertEquals("VARIANT", rowType.getTypeAt(1).getTypeRoot().name());
    } else {
      UnsupportedOperationException ex = assertThrows(
          UnsupportedOperationException.class,
          () -> ParquetSchemaConverter.convertToRowType(variantParquet));
      assertTrue(ex.getMessage().contains("VARIANT type is only supported in Flink 2.1+"));
    }
  }

  @Test
  void testNestedVariantInArrayStructuralFallback() {
    MessageType nestedVariantParquet = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
            Type.Repetition.REQUIRED).named("id"),
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.listType())
            .addField(Types.repeatedGroup()
                .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                        Type.Repetition.REQUIRED).named("metadata"))
                    .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                        Type.Repetition.REQUIRED).named("value"))
                    .named("element"))
                .named("list"))
            .named("variants"));

    if (HoodieSchemaConverter.tryCreateVariantDataType() != null) {
      RowType rowType = ParquetSchemaConverter.convertToRowType(nestedVariantParquet);
      assertNotNull(rowType);
      assertEquals(2, rowType.getFieldCount());
      LogicalType variantsType = rowType.getTypeAt(1);
      assertInstanceOf(ArrayType.class, variantsType);
      assertEquals("VARIANT", ((ArrayType) variantsType).getElementType().getTypeRoot().name());
    } else {
      UnsupportedOperationException ex = assertThrows(
          UnsupportedOperationException.class,
          () -> ParquetSchemaConverter.convertToRowType(nestedVariantParquet));
      assertTrue(ex.getMessage().contains("VARIANT type is only supported in Flink 2.1+"));
    }
  }

  /**
   * Tests that {@link ParquetSchemaConverter#hasVariantAnnotation} correctly detects
   * the VARIANT annotation when parquet-java 1.15.2+ is on the classpath, and returns
   * false otherwise.
   */
  @Test
  void testHasVariantAnnotation() {
    assertFalse(ParquetSchemaConverter.hasVariantAnnotation(null));
    assertFalse(ParquetSchemaConverter.hasVariantAnnotation(LogicalTypeAnnotation.stringType()));
    assertFalse(ParquetSchemaConverter.hasVariantAnnotation(LogicalTypeAnnotation.listType()));

    LogicalTypeAnnotation variantAnnotation = tryCreateVariantAnnotation();
    if (variantAnnotation != null) {
      assertTrue(ParquetSchemaConverter.hasVariantAnnotation(variantAnnotation));
    }
  }

  /**
   * Tests that an annotated shredded variant group (VARIANT annotation + typed_value field)
   * throws. Only runs when parquet-java 1.15.2+ provides VariantLogicalTypeAnnotation.
   */
  @Test
  void testAnnotatedShreddedVariantThrows() {
    LogicalTypeAnnotation variantAnnotation = tryCreateVariantAnnotation();
    if (variantAnnotation == null) {
      // parquet-java version doesn't support VARIANT annotation; shredded groups without
      // annotation are treated as generic ROW (3 fields, structural check won't match).
      return;
    }

    MessageType shreddedVariantParquet = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
            Type.Repetition.REQUIRED).named("id"),
        Types.buildGroup(Type.Repetition.REQUIRED)
            .as(variantAnnotation)
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("metadata"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.OPTIONAL).named("value"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.OPTIONAL).named("typed_value"))
            .named("data"));

    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> ParquetSchemaConverter.convertToRowType(shreddedVariantParquet));
    assertTrue(ex.getMessage().contains("Shredded Variant is not supported"));
    assertTrue(ex.getMessage().contains("typed_value"));
  }

  /**
   * Unannotated group with metadata + value + typed_value (3 fields) does not match the
   * structural fallback (which requires exactly 2 fields), so it is treated as a generic ROW.
   */
  @Test
  void testUnannotatedShreddedGroupTreatedAsRow() {
    MessageType shreddedNoAnnotation = new MessageType(
        "test",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
            Type.Repetition.REQUIRED).named("id"),
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("metadata"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
                Type.Repetition.REQUIRED).named("value"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32,
                Type.Repetition.OPTIONAL).named("typed_value"))
            .named("data"));

    RowType rowType = ParquetSchemaConverter.convertToRowType(shreddedNoAnnotation);
    assertEquals(2, rowType.getFieldCount());
    assertEquals("ROW", rowType.getTypeAt(1).getTypeRoot().name());
  }

  /**
   * Tries to create a {@code VariantLogicalTypeAnnotation} via reflection.
   * Returns null if the class is not on the classpath (parquet-java < 1.15.2).
   */
  private static LogicalTypeAnnotation tryCreateVariantAnnotation() {
    try {
      Method variantType = LogicalTypeAnnotation.class.getMethod("variantType", byte.class);
      return (LogicalTypeAnnotation) variantType.invoke(null, (byte) 1);
    } catch (NoSuchMethodException e) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create VariantLogicalTypeAnnotation via reflection", e);
    }
  }
}
