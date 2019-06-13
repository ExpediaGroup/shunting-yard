/**
 * Copyright (C) 2016-2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.shuntingyard.replicator.exec.event.aggregation;

import static java.lang.Boolean.TRUE;

import static org.apache.hadoop.hive.common.StatsSetupConst.CASCADE;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;

import com.expediagroup.shuntingyard.replicator.exec.event.MetaStoreEvent;
import com.expediagroup.shuntingyard.replicator.exec.event.MetaStoreEvent.Builder;
import com.google.common.collect.ImmutableMap;

class EventMerger {

  EventMerger() {}

  public boolean canMerge(MetaStoreEvent a, MetaStoreEvent b) {
    return (Objects.equals(a.getEventType(), b.getEventType()) || (!a.isDropEvent() && !b.isDropEvent()))
        && Objects.equals(a.getQualifiedTableName(), b.getQualifiedTableName())
        && Objects.equals(a.getPartitionColumns(), b.getPartitionColumns());
  }

  // Note this method creates a new object each time it's invoked and this may end-up generating a lot
  // of garbage. However the number of expected events to be merged is low so this should not be an issue. Keep an eye
  // on this anyway.
  public MetaStoreEvent merge(MetaStoreEvent a, MetaStoreEvent b) {
    checkArgument(canMerge(a, b), "Events cannot be merged");
    // Event type of the first event is kept for the new event
    Builder builder = MetaStoreEvent
        .builder(a.getEventType(), a.getDatabaseName(), a.getTableName(), a.getReplicaDatabaseName(),
            a.getReplicaTableName())
        .parameters(a.getParameters())
        .parameters(b.getParameters())
        .environmentContext(a.getEnvironmentContext())
        .environmentContext(b.getEnvironmentContext());
    if (a.getPartitionColumns() != null) {
      builder.partitionColumns(a.getPartitionColumns());
    }
    if (a.getPartitionValues() != null) {
      a.getPartitionValues().stream().forEach(builder::partitionValues);
    }
    if (b.getPartitionValues() != null) {
      b.getPartitionValues().stream().forEach(builder::partitionValues);
    }
    // Ensure that cascade is preserved
    if (a.isCascade() || b.isCascade()) {
      builder.environmentContext(ImmutableMap.of(CASCADE, TRUE.toString()));
    }
    return builder.build();
  }

}
