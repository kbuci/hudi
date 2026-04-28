/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tool class used to convert from {@link RowData} to Avro {@link GenericRecord}.
 *
 * <p>NOTE: reference from Flink release 1.12.0, should remove when Flink version upgrade to that.
 */
@Internal
public class RowDataToAvroConverters {

  private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  // --------------------------------------------------------------------------------
  // Runtime Converters
  // --------------------------------------------------------------------------------

  /**
   * Runtime converter that converts objects of Flink Table & SQL internal data structures to
   * corresponding Avro data structures.
   */
  @FunctionalInterface
  public interface RowDataToAvroConverter extends Serializable {
    Object convert(HoodieSchema schema, Object object);
  }

  // --------------------------------------------------------------------------------
  // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
  // necessary because the maven shade plugin cannot relocate classes in
  // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
  // sql-client uber jars.
  // --------------------------------------------------------------------------------

  /**
   * Creates a runtime converter according to the given logical type that converts objects of
   * Flink Table & SQL internal data structures to corresponding Avro data structures.
   */
  public static RowDataToAvroConverter createConverter(LogicalType type) {
    return createConverter(type, true);
  }

  public static RowDataToAvroConverter createConverter(LogicalType type, boolean utcTimezone) {
    final RowDataToAvroConverter converter;
    switch (type.getTypeRoot()) {
      case NULL:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                return null;
              }
            };
        break;
      case TINYINT:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                return ((Byte) object).intValue();
              }
            };
        break;
      case SMALLINT:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                return ((Short) object).intValue();
              }
            };
        break;
      case BOOLEAN: // boolean
      case INTEGER: // int
      case INTERVAL_YEAR_MONTH: // long
      case BIGINT: // long
      case INTERVAL_DAY_TIME: // long
      case FLOAT: // float
      case DOUBLE: // double
      case TIME_WITHOUT_TIME_ZONE: // int
      case DATE: // int
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                return object;
              }
            };
        break;
      case CHAR:
      case VARCHAR:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                return new Utf8(((BinaryStringData) object).toBytes());
              }
            };
        break;
      case BINARY:
      case VARBINARY:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                return ByteBuffer.wrap((byte[]) object);
              }
            };
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        int precision = RowDataUtils.precision(type);
        if (precision <= 3) {
          converter = new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(HoodieSchema schema, Object object) {
                return ((TimestampData) object).toInstant().toEpochMilli();
              }
          };
        } else if (precision <= 6) {
          converter = new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(HoodieSchema schema, Object object) {
              Instant instant = ((TimestampData) object).toInstant();
              return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000_000), instant.getNano() / 1000);
            }
          };
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision: " + precision);
        }
        break;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        precision = RowDataUtils.precision(type);
        if (precision <= 3) {
          converter =
              new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(HoodieSchema schema, Object object) {
                  return utcTimezone ? ((TimestampData) object).toInstant().toEpochMilli() : ((TimestampData) object).toTimestamp().getTime();
                }
              };
        } else if (precision <= 6) {
          converter =
              new RowDataToAvroConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object convert(HoodieSchema schema, Object object) {
                  Instant instant = utcTimezone ? ((TimestampData) object).toInstant() : ((TimestampData) object).toTimestamp().toInstant();
                  return  Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000_000), instant.getNano() / 1000);
                }
              };
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision: " + precision);
        }
        break;
      case DECIMAL:
        converter =
            new RowDataToAvroConverter() {
              private static final long serialVersionUID = 1L;

              @Override
              public Object convert(HoodieSchema schema, Object object) {
                BigDecimal javaDecimal = ((DecimalData) object).toBigDecimal();
                return DECIMAL_CONVERSION.toFixed(javaDecimal, schema.toAvroSchema(), schema.toAvroSchema().getLogicalType());
              }
            };
        break;
      case ARRAY:
        converter = createArrayConverter((ArrayType) type, utcTimezone);
        break;
      case ROW:
        converter = createRowConverter((RowType) type, utcTimezone);
        break;
      case MAP:
      case MULTISET:
        converter = createMapConverter(type, utcTimezone);
        break;
      case RAW:
      default:
        // Flink 2.1+ introduces VARIANT as a first-class LogicalTypeRoot. When present,
        // the runtime provides a Variant object (via RowData.getVariant()) from which we
        // can extract raw metadata/value bytes directly — no HoodieSchema inspection is
        // needed to distinguish Variant from a plain ROW.
        //
        // We detect it by name rather than by enum constant because hudi-flink-client is
        // compiled against Flink 1.20 (where LogicalTypeRoot.VARIANT does not exist).
        //
        // For older Flink versions the Variant arrives as ROW<metadata BYTES, value BYTES>
        // and the ROW case above handles conversion correctly because HoodieSchema carries
        // the VARIANT type information that Flink's RowType cannot express (see
        // convertVariant() in HoodieSchemaConverter).
        if ("VARIANT".equals(type.getTypeRoot().name())) {
          converter = createVariantConverter();
          break;
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    // wrap into nullable converter
    return new RowDataToAvroConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(HoodieSchema schema, Object object) {
        if (object == null) {
          return null;
        }

        // get actual schema if it is a nullable schema
        HoodieSchema actualSchema;
        if (schema.getType() == HoodieSchemaType.UNION) {
          List<HoodieSchema> types = schema.getTypes();
          int size = types.size();
          if (size == 2 && types.get(1).getType() == HoodieSchemaType.NULL) {
            actualSchema = types.get(0);
          } else if (size == 2 && types.get(0).getType() == HoodieSchemaType.NULL) {
            actualSchema = types.get(1);
          } else {
            throw new IllegalArgumentException(
                "The Avro schema is not a nullable type: " + schema);
          }
        } else {
          actualSchema = schema;
        }
        return converter.convert(actualSchema, object);
      }
    };
  }

  /**
   * Creates a converter for Flink 2.1+ VARIANT LogicalType. The converter receives a Flink
   * {@code Variant} object at runtime and extracts the raw metadata/value byte arrays via
   * reflection, then packs them into an Avro GenericRecord with the Variant schema.
   *
   * <p>Reflection is required because the {@code Variant} interface and {@code BinaryVariant}
   * class only exist in Flink 2.1+, while this module compiles against Flink 1.20.
   */
  private static RowDataToAvroConverter createVariantConverter() {
    return new RowDataToAvroConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(HoodieSchema schema, Object object) {
        try {
          java.lang.reflect.Method metadataMethod = object.getClass().getMethod("getMetadata");
          java.lang.reflect.Method valueMethod = object.getClass().getMethod("getValue");
          byte[] metadata = (byte[]) metadataMethod.invoke(object);
          byte[] value = (byte[]) valueMethod.invoke(object);

          final GenericRecord record = new GenericData.Record(schema.toAvroSchema());
          record.put("metadata", ByteBuffer.wrap(metadata));
          record.put("value", ByteBuffer.wrap(value));
          return record;
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to extract Variant fields via reflection. "
                  + "VARIANT LogicalType requires Flink 2.1+.",
              e);
        }
      }
    };
  }

  private static RowDataToAvroConverter createRowConverter(RowType rowType, boolean utcTimezone) {
    final RowDataToAvroConverter[] fieldConverters =
        rowType.getChildren().stream()
            .map(type -> createConverter(type, utcTimezone))
            .toArray(RowDataToAvroConverter[]::new);
    final LogicalType[] fieldTypes =
        rowType.getFields().stream()
            .map(RowType.RowField::getType)
            .toArray(LogicalType[]::new);
    final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
    for (int i = 0; i < fieldTypes.length; i++) {
      fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
    }
    final int length = rowType.getFieldCount();

    return new RowDataToAvroConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(HoodieSchema schema, Object object) {
        if (schema.getType() == HoodieSchemaType.VARIANT) {
          return convertVariantToAvro(schema, (RowData) object);
        }
        final RowData row = (RowData) object;
        final List<HoodieSchemaField> fields = schema.getFields();
        final GenericRecord record = new GenericData.Record(schema.toAvroSchema());
        for (int i = 0; i < length; ++i) {
          final HoodieSchemaField schemaField = fields.get(i);
          Object avroObject =
              fieldConverters[i].convert(
                  schemaField.schema(), fieldGetters[i].getFieldOrNull(row));
          record.put(i, avroObject);
        }
        return record;
      }
    };
  }

  /**
   * Converts a Flink ROW (representing an unshredded Variant) directly to an Avro GenericRecord
   * with the variant logicalType annotation. This bypasses the generic row converter to guard
   * against field-count mismatches between the Flink ROW (always 2 fields: metadata, value)
   * and the Avro Variant schema (which may have 3 fields for shredded Variants).
   */
  private static GenericRecord convertVariantToAvro(HoodieSchema variantSchema, RowData row) {
    if (variantSchema instanceof HoodieSchema.Variant
        && ((HoodieSchema.Variant) variantSchema).isShredded()) {
      throw new UnsupportedOperationException(
          "Shredded Variant is not yet supported in Flink. Use unshredded Variant instead.");
    }
    GenericRecord record = new GenericData.Record(variantSchema.toAvroSchema());
    byte[] metadata = row.isNullAt(0) ? null : row.getBinary(0);
    byte[] value = row.isNullAt(1) ? null : row.getBinary(1);
    record.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD,
        metadata != null ? ByteBuffer.wrap(metadata) : null);
    record.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD,
        value != null ? ByteBuffer.wrap(value) : null);
    return record;
  }

  private static RowDataToAvroConverter createArrayConverter(ArrayType arrayType, boolean utcTimezone) {
    LogicalType elementType = arrayType.getElementType();
    final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
    final RowDataToAvroConverter elementConverter = createConverter(arrayType.getElementType(), utcTimezone);

    return new RowDataToAvroConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(HoodieSchema schema, Object object) {
        final HoodieSchema elementSchema = schema.getElementType();
        ArrayData arrayData = (ArrayData) object;
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < arrayData.size(); ++i) {
          list.add(
              elementConverter.convert(
                  elementSchema, elementGetter.getElementOrNull(arrayData, i)));
        }
        return list;
      }
    };
  }

  private static RowDataToAvroConverter createMapConverter(LogicalType type, boolean utcTimezone) {
    LogicalType valueType = HoodieSchemaConverter.extractValueTypeToMap(type);
    final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
    final RowDataToAvroConverter valueConverter = createConverter(valueType, utcTimezone);

    return new RowDataToAvroConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(HoodieSchema schema, Object object) {
        final HoodieSchema valueSchema = schema.getValueType();
        final MapData mapData = (MapData) object;
        final ArrayData keyArray = mapData.keyArray();
        final ArrayData valueArray = mapData.valueArray();
        final Map<Object, Object> map = new HashMap<>(mapData.size());
        for (int i = 0; i < mapData.size(); ++i) {
          final String key = keyArray.getString(i).toString();
          final Object value =
              valueConverter.convert(
                  valueSchema, valueGetter.getElementOrNull(valueArray, i));
          map.put(key, value);
        }
        return map;
      }
    };
  }
}

