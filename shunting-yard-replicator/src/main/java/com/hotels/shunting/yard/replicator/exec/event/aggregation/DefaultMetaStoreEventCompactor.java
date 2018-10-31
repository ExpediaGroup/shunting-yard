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

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

class DefaultMetaStoreEventCompactor {
  private static final Logger log = LoggerFactory.getLogger(DefaultMetaStoreEventCompactor.class);

  private final EventMerger eventMerger;

  DefaultMetaStoreEventCompactor() {
    this(new EventMerger());
  }

  DefaultMetaStoreEventCompactor(EventMerger eventMerger) {
    this.eventMerger = eventMerger;
  }

  /**
   * Reduces the number of events to the minimum necessary to execute Circus Train as few times as possible.
   * <p>
   * This method assumes all the events passed in are for the same table and are sorted in chronological order, i.e.
   * they come from the same source table.
   * <p>
   * Basic rules:
   * <ol>
   * <li>Drop events are always kept untouched</li>
   * <li>Any create/alter event before a drop event is removed from the final list of events</li>
   * <li>Create and alter events are aggregated together</li>
   * <li>If an alter table has been issue with the CASCADE the CASCADE option of the aggregated event will on as
   * well</li>
   * <li>Parameters from previous events which are also in later events will be overwritten with the most recent value.
   * This statement is also true for the environment context</li>
   * </ol>
   *
   * @param tableEvents a chronological ordered list of events on a single table
   * @return compacted set of events
   */
  public List<MetaStoreEvent> compact(List<MetaStoreEvent> tableEvents) {
    LinkedList<MetaStoreEvent> finalEvents = new LinkedList<>();
    for (MetaStoreEvent e : tableEvents) {
      switch (e.getEventType()) {
      case DROP_TABLE:
        // Keep only previous drop events
        processDropTable(finalEvents, e);
        break;
      case DROP_PARTITION:
        mergeOrAdd(finalEvents, e, evt -> evt.isDropEvent());
        break;
      case CREATE_TABLE:
      case ADD_PARTITION:
      case ALTER_PARTITION:
      case INSERT:
        mergeOrAdd(finalEvents, e, evt -> !evt.isDropEvent());
        break;
      default:
        log.debug("Unknown event type {}: adding to list of event", e.getEventType());
        finalEvents.add(e);
        break;
      }
    }
    return finalEvents;
  }

  private void processDropTable(LinkedList<MetaStoreEvent> finalEvents, MetaStoreEvent event) {
    ListIterator<MetaStoreEvent> it = finalEvents.listIterator();
    while (it.hasNext()) {
      MetaStoreEvent e = it.next();
      if (e.isDropEvent()) {
        continue;
      }
      it.remove();
    }
    finalEvents.add(event);
  }

  private void mergeOrAdd(
      LinkedList<MetaStoreEvent> events,
      MetaStoreEvent event,
      Predicate<MetaStoreEvent> matchPredicate) {
    ListIterator<MetaStoreEvent> it = events.listIterator(events.size());
    MetaStoreEvent previousEvent = null;
    boolean found = false;
    while (!found && it.hasPrevious()) {
      previousEvent = it.previous();
      found = matchPredicate.test(previousEvent);
    }
    if (found && eventMerger.canMerge(previousEvent, event)) {
      it.set(eventMerger.merge(previousEvent, event));
    } else {
      events.add(event);
    }
  }

}
