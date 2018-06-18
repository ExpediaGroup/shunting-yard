package com.hotels.shunting.yard.common.event.aggregation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

public class DefaultSerializableListenerEventAggregator implements SerializableListenerEventAggregator {

  private final DefaultSerializableListenerEventCompactor compactor;

  public DefaultSerializableListenerEventAggregator() {
    this(new DefaultSerializableListenerEventCompactor());
  }

  public DefaultSerializableListenerEventAggregator(DefaultSerializableListenerEventCompactor compactor) {
    this.compactor = compactor;
  }

  @Override
  public List<SerializableListenerEvent> aggregate(List<SerializableListenerEvent> events) {
    Map<String, List<SerializableListenerEvent>> eventsPerTable = mapEvents(events);
    List<SerializableListenerEvent> aggregatedEvents = new ArrayList<>();
    for (List<SerializableListenerEvent> tableEvents : eventsPerTable.values()) {
      aggregatedEvents.addAll(compactor.compact(tableEvents));
    }
    return aggregatedEvents;
  }

  private Map<String, List<SerializableListenerEvent>> mapEvents(List<SerializableListenerEvent> events) {
    Map<String, List<SerializableListenerEvent>> eventsPerTable = new LinkedHashMap<>();
    for (SerializableListenerEvent e : events) {
      String qName = e.getQualifiedTableName();
      List<SerializableListenerEvent> tableEvents = eventsPerTable.get(qName);
      if (tableEvents == null) {
        tableEvents = new ArrayList<>();
        eventsPerTable.put(qName, tableEvents);
      }
      tableEvents.add(e);
    }
    return eventsPerTable;
  }

}
