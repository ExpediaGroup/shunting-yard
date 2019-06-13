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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.expediagroup.shuntingyard.replicator.exec.event.MetaStoreEvent;

public class DefaultMetaStoreEventAggregator implements MetaStoreEventAggregator {

  private final DefaultMetaStoreEventCompactor compactor;

  public DefaultMetaStoreEventAggregator() {
    this(new DefaultMetaStoreEventCompactor());
  }

  public DefaultMetaStoreEventAggregator(DefaultMetaStoreEventCompactor compactor) {
    this.compactor = compactor;
  }

  @Override
  public List<MetaStoreEvent> aggregate(List<MetaStoreEvent> events) {
    Map<String, List<MetaStoreEvent>> eventsPerTable = mapEvents(events);
    List<MetaStoreEvent> aggregatedEvents = new ArrayList<>();
    for (List<MetaStoreEvent> tableEvents : eventsPerTable.values()) {
      aggregatedEvents.addAll(compactor.compact(tableEvents));
    }
    return aggregatedEvents;
  }

  private Map<String, List<MetaStoreEvent>> mapEvents(List<MetaStoreEvent> events) {
    Map<String, List<MetaStoreEvent>> eventsPerTable = new LinkedHashMap<>();
    for (MetaStoreEvent e : events) {
      String qName = e.getQualifiedTableName();
      List<MetaStoreEvent> tableEvents = eventsPerTable.get(qName);
      if (tableEvents == null) {
        tableEvents = new ArrayList<>();
        eventsPerTable.put(qName, tableEvents);
      }
      tableEvents.add(e);
    }
    return eventsPerTable;
  }

}
