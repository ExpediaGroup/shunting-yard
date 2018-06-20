/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.exec.event.aggregation;

import static com.google.common.base.Preconditions.checkArgument;

import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_TABLE;

import java.util.Objects;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent.Builder;

class EventMerger {

  EventMerger() {}

  private boolean isDropEvent(MetaStoreEvent event) {
    return event.getEventType() == ON_DROP_PARTITION || event.getEventType() == ON_DROP_TABLE;
  }

  public boolean canMerge(MetaStoreEvent a, MetaStoreEvent b) {
    return (Objects.equals(a.getEventType(), b.getEventType()) || (!isDropEvent(a) && !isDropEvent(b)))
        && Objects.equals(a.getQualifiedTableName(), b.getQualifiedTableName())
        && Objects.equals(a.getPartitionColumns(), b.getPartitionColumns());
  }

  // Note this methods creates a new object each time it's invoked and this may end-up generating a lot
  // of garbage. However the number of expected events to be merged is low so this should not be an issue. Keep an eye
  // on this anyway.
  public MetaStoreEvent merge(MetaStoreEvent a, MetaStoreEvent b) {
    checkArgument(canMerge(a, b), "Events cannot be merged");
    // Event type of the first event is kept for the new event
    Builder builder = MetaStoreEvent
        .builder(a.getEventType(), a.getDatabaseName(), a.getTableName())
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
    return builder.build();
  }

}
