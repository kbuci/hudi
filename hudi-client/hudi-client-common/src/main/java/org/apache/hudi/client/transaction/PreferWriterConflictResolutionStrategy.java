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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictAwaitingIngestionInflightException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * This class extends the base implementation of conflict resolution strategy.
 * It gives preference to non-blocking ingestion over table services in case of conflicts.
 */
@Slf4j
public class PreferWriterConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  /**
   * For tableservices like replacecommit and compaction commits this method also returns ingestion inflight commits.
   */
  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant) {
    return getCandidateInstants(metaClient, currentInstant, lastSuccessfulInstant, Option.empty());
  }

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant, Option<HoodieWriteConfig> writeConfigOpt) {
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    boolean isClustering = ClusteringUtils.isClusteringInstant(activeTimeline, currentInstant, metaClient.getInstantGenerator());
    if (isClustering || COMPACTION_ACTION.equals(currentInstant.getAction())) {
      return getCandidateInstantsForTableServicesCommits(activeTimeline, currentInstant, isClustering, metaClient, writeConfigOpt);
    } else {
      return getCandidateInstantsForNonTableServicesCommits(activeTimeline, currentInstant);
    }
  }

  private Stream<HoodieInstant> getCandidateInstantsForNonTableServicesCommits(HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant) {

    // To find out which instants are conflicting, we apply the following logic
    // Get all the completed instants timeline only for commits that have happened
    // since the last successful write based on the transition times.
    // We need to check for write conflicts since they may have mutated the same files
    // that are being newly created by the current write.
    List<HoodieInstant> completedCommitsInstants = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
        .filterCompletedInstants()
        .findInstantsModifiedAfterByCompletionTime(currentInstant.requestedTime())
        .getInstantsOrderedByCompletionTime()
        .collect(Collectors.toList());
    log.info("Instants that may have conflict with {} are {}", currentInstant, completedCommitsInstants);
    return completedCommitsInstants.stream();
  }

  /**
   * To find which instants are conflicting, we apply the following logic
   * Get both completed instants and ingestion inflight commits that have happened since the last successful write.
   * We need to check for write conflicts since they may have mutated the same files
   * that are being newly created by the current write.
   *
   * <p>If the current write is clustering and blocking for pending ingestion is enabled, additionally
   * checks all ingestion .requested instants and asserts that they belong to failed writes (expired heartbeat).
   * Otherwise raises {@link HoodieWriteConflictAwaitingIngestionInflightException} since we do not want
   * to risk committing clustering while an active ingestion write may conflict.</p>
   */
  private Stream<HoodieInstant> getCandidateInstantsForTableServicesCommits(
      HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant,
      boolean isCurrentOperationClustering, HoodieTableMetaClient metaClient,
      Option<HoodieWriteConfig> writeConfigOpt) {

    // Fetch list of completed commits.
    Stream<HoodieInstant> completedCommitsStream =
        activeTimeline
            .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
            .filterCompletedInstants()
            .findInstantsModifiedAfterByCompletionTime(currentInstant.requestedTime())
            .getInstantsAsStream();

    // Determine blocking config and heartbeat timeout from write config (or defaults).
    boolean blockForPendingIngestion;
    long maxHeartbeatIntervalMs;
    if (writeConfigOpt.isPresent()) {
      HoodieWriteConfig writeConfig = writeConfigOpt.get();
      blockForPendingIngestion = writeConfig.isClusteringBlockForPendingIngestion();
      maxHeartbeatIntervalMs = writeConfig.getHoodieClientHeartbeatIntervalInMs()
          * (writeConfig.getHoodieClientHeartbeatTolerableMisses() + 1);
    } else {
      blockForPendingIngestion = (boolean) HoodieWriteConfig.CLUSTERING_BLOCK_FOR_PENDING_INGESTION.defaultValue();
      maxHeartbeatIntervalMs = (long) (int) HoodieWriteConfig.CLIENT_HEARTBEAT_INTERVAL_IN_MS.defaultValue()
          * ((int) HoodieWriteConfig.CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.defaultValue() + 1);
    }

    // Fetch list of ingestion inflight commits.
    // If the current write is clustering and blocking is enabled, also check ingestion .requested
    // instants and verify they belong to failed writes (expired heartbeat). If a .requested instant
    // has an active heartbeat, throw to abort clustering rather than risk conflicting with ingestion.
    Stream<HoodieInstant> inflightIngestionCommitsStream;
    if (isCurrentOperationClustering && blockForPendingIngestion) {
      inflightIngestionCommitsStream = activeTimeline
          .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION))
          .filterInflightsAndRequested()
          .getInstantsAsStream()
          .filter(i -> !ClusteringUtils.isClusteringInstant(activeTimeline, i, metaClient.getInstantGenerator()))
          .filter(i -> {
            if (i.isRequested()) {
              try {
                if (HoodieHeartbeatUtils.isHeartbeatExpired(i.requestedTime(), maxHeartbeatIntervalMs,
                    metaClient.getStorage(), metaClient.getBasePath().toString())) {
                  return false;
                } else {
                  throw new HoodieWriteConflictAwaitingIngestionInflightException(
                      String.format("Pending ingestion instant %s with active heartbeat "
                          + "has not transitioned to inflight yet but may potentially conflict with current clustering", i));
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
            return i.isInflight();
          });
    } else {
      inflightIngestionCommitsStream = activeTimeline
          .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION))
          .filterInflights()
          .getInstantsAsStream();
    }

    // Merge and sort the instants and return.
    List<HoodieInstant> instantsToConsider = Stream.concat(completedCommitsStream, inflightIngestionCommitsStream)
        .sorted(Comparator.comparing(o -> o.getCompletionTime()))
        .collect(Collectors.toList());
    log.info("Instants that may have conflict with {} are {}", currentInstant, instantsToConsider);
    return instantsToConsider.stream();
  }

  @Override
  public boolean isPreCommitRequired() {
    return true;
  }
}
