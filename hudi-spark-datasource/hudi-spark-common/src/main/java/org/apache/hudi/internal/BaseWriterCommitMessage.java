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

package org.apache.hudi.internal;

import org.apache.hudi.client.WriteStatus;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for HoodieWriterCommitMessage used by Spark datasource v2.
 */
public class BaseWriterCommitMessage implements Serializable {

  private final List<WriteStatus> writeStatuses;

  public BaseWriterCommitMessage(List<WriteStatus> writeStatuses) {
    this.writeStatuses = writeStatuses;
  }

  public List<WriteStatus> getWriteStatuses() {
    return writeStatuses;
  }

  @Override
  public String toString() {
    return "HoodieWriterCommitMessage{" + "writeStatuses=" + Arrays.toString(writeStatuses.toArray()) + '}';
  }
}
