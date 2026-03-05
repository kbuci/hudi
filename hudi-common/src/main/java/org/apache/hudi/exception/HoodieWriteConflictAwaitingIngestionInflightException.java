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

package org.apache.hudi.exception;

/**
 * Thrown when a clustering write detects a pending ingestion instant (in requested state)
 * with an active heartbeat that has not yet transitioned to inflight. Since ingestion has
 * precedence over clustering, the clustering commit is aborted to avoid risking a conflict
 * with the ongoing ingestion write.
 */
public class HoodieWriteConflictAwaitingIngestionInflightException extends HoodieWriteConflictException {

  public HoodieWriteConflictAwaitingIngestionInflightException(String msg) {
    super(msg);
  }

  public HoodieWriteConflictAwaitingIngestionInflightException(Throwable e) {
    super(e);
  }

  public HoodieWriteConflictAwaitingIngestionInflightException(String msg, Throwable e) {
    super(msg, e);
  }
}
