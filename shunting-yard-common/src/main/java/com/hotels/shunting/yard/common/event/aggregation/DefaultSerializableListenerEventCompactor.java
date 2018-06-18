package com.hotels.shunting.yard.common.event.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

class DefaultSerializableListenerEventCompactor {
  private static final Logger log = LoggerFactory.getLogger(DefaultSerializableListenerEventCompactor.class);

  DefaultSerializableListenerEventCompactor() {}

  /**
   * Reduces the number of event to the minimum necessary to execute Circus Train as fewer times as possible.
   * <p>
   * This method assumes all the events passed in are for the same table and are sorted in chronological order, i.e.
   * they are from the same source.
   *
   * @param tableEvents a chronological ordered list of event on a single table
   * @return reduced set of events
   */
  public List<SerializableListenerEvent> compact(List<SerializableListenerEvent> tableEvents) {
    List<SerializableListenerEvent> finalEvents = new ArrayList<>(tableEvents.size());
    for (SerializableListenerEvent e : tableEvents) {
      boolean containsCreateTable = false;
      switch (e.getEventType()) {
      case ON_DROP_TABLE:
        finalEvents.clear();
        finalEvents.add(e);
        containsCreateTable = false;
        break;
      case ON_DROP_PARTITION: {
        SerializableDropPartitionEvent dropPartitionEvent = (SerializableDropPartitionEvent) e;
        Optional<SerializableAddPartitionEvent> optPreviousAlter = findAlterParition(finalEvents);
        if (optPreviousAlter.isPresent()) {
          SerializableAddPartitionEvent previousAlter = optPreviousAlter.get();
          previousAlter.getPartitions().removeAll(dropPartitionEvent.getPartitions());
          if (previousAlter.getPartitions().isEmpty()) {
            finalEvents.remove(previousAlter);
          }
        }
        finalEvents.add(e);
        break;
      }
      case ON_CREATE_TABLE:
        containsCreateTable = true;
        finalEvents.add(e);
        break;
      case ON_ADD_PARTITION: {
        if (containsCreateTable) {
          // Create table will try to copy all partitions
          break;
        }
        SerializableAddPartitionEvent addPartitionEvent = (SerializableAddPartitionEvent) e;
        Optional<SerializableAddPartitionEvent> previousAlter = findAlterParition(finalEvents);
        if (previousAlter.isPresent()) {
          merge(previousAlter.get(), addPartitionEvent);
        } else {
          finalEvents.add(e);
        }
        break;
      }
      case ON_ALTER_TABLE:
      case ON_ALTER_PARTITION:
        if (containsCreateTable) {
          // Create table will try to copy all partitions
          break;
        }
        break;
      default:
        log.debug("Unknown event type {}: adding to list of event", e.getEventType());
        finalEvents.add(e);
        break;
      }
    }
    return finalEvents;
  }

  private Optional<SerializableAddPartitionEvent> findAlterParition(List<SerializableListenerEvent> finalEvents) {
    return finalEvents
        .stream()
        .filter(e -> e.getEventType() == EventType.ON_ALTER_PARTITION)
        .map(e -> (SerializableAddPartitionEvent) e)
        .findFirst();
  }

  private SerializableAddPartitionEvent merge(SerializableAddPartitionEvent into, SerializableAddPartitionEvent other) {
    // TODO
    return null;
  }

}
