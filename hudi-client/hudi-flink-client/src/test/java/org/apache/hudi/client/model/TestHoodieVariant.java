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

import org.apache.flink.table.data.GenericRowData;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HoodieVariant}.
 */
public class TestHoodieVariant {

  private static final byte[] SAMPLE_METADATA = new byte[] {0x01, 0x02, 0x03};
  private static final byte[] SAMPLE_VALUE = new byte[] {0x0A, 0x0B};

  @Test
  public void testConstructorAndGetters() {
    HoodieVariant variant = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);
    assertArrayEquals(SAMPLE_METADATA, variant.getMetadata());
    assertArrayEquals(SAMPLE_VALUE, variant.getValue());
  }

  @Test
  public void testConstructorWithNulls() {
    HoodieVariant variant = new HoodieVariant(null, null);
    assertNull(variant.getMetadata());
    assertNull(variant.getValue());
  }

  @Test
  public void testEqualsAndHashCodeSameContent() {
    HoodieVariant v1 = new HoodieVariant(
        new byte[] {0x01, 0x02}, new byte[] {0x0A});
    HoodieVariant v2 = new HoodieVariant(
        new byte[] {0x01, 0x02}, new byte[] {0x0A});

    assertEquals(v1, v2);
    assertEquals(v1.hashCode(), v2.hashCode());
  }

  @Test
  public void testEqualsSameInstance() {
    HoodieVariant v1 = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);
    assertEquals(v1, v1);
  }

  @Test
  public void testNotEqualsDifferentMetadata() {
    HoodieVariant v1 = new HoodieVariant(new byte[] {0x01}, SAMPLE_VALUE);
    HoodieVariant v2 = new HoodieVariant(new byte[] {0x02}, SAMPLE_VALUE);
    assertNotEquals(v1, v2);
  }

  @Test
  public void testNotEqualsDifferentValue() {
    HoodieVariant v1 = new HoodieVariant(SAMPLE_METADATA, new byte[] {0x0A});
    HoodieVariant v2 = new HoodieVariant(SAMPLE_METADATA, new byte[] {0x0B});
    assertNotEquals(v1, v2);
  }

  @Test
  public void testNotEqualsNull() {
    HoodieVariant v1 = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);
    assertNotEquals(null, v1);
  }

  @Test
  public void testNotEqualsDifferentType() {
    HoodieVariant v1 = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);
    assertNotEquals("not a variant", v1);
  }

  @Test
  public void testToString() {
    HoodieVariant variant = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);
    String s = variant.toString();
    assertTrue(s.contains("3 bytes"), "Should show metadata size");
    assertTrue(s.contains("2 bytes"), "Should show value size");
  }

  @Test
  public void testToStringWithNulls() {
    HoodieVariant variant = new HoodieVariant(null, null);
    String s = variant.toString();
    assertTrue(s.contains("null"));
  }

  @Test
  public void testFromRowData() {
    GenericRowData rowData = GenericRowData.of(SAMPLE_METADATA, SAMPLE_VALUE);
    HoodieVariant variant = HoodieVariant.fromRowData(rowData);

    assertArrayEquals(SAMPLE_METADATA, variant.getMetadata());
    assertArrayEquals(SAMPLE_VALUE, variant.getValue());
  }

  @Test
  public void testFromRowDataWithNullFields() {
    GenericRowData rowData = GenericRowData.of(null, null);
    HoodieVariant variant = HoodieVariant.fromRowData(rowData);

    assertNull(variant.getMetadata());
    assertNull(variant.getValue());
  }

  @Test
  public void testFromRowDataPreservesEquality() {
    GenericRowData rowData = GenericRowData.of(SAMPLE_METADATA, SAMPLE_VALUE);
    HoodieVariant fromRow = HoodieVariant.fromRowData(rowData);
    HoodieVariant direct = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);

    assertEquals(direct, fromRow);
    assertEquals(direct.hashCode(), fromRow.hashCode());
  }

  @Test
  public void testToFlinkVariantThrowsOnOlderFlink() {
    // Default test classpath uses Flink 1.20 stub where BinaryVariant does not exist.
    HoodieVariant variant = new HoodieVariant(SAMPLE_METADATA, SAMPLE_VALUE);
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        variant::toFlinkVariant);
    assertTrue(ex.getMessage().contains("Flink 2.1+"));
  }
}
