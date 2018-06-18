package com.hotels.shunting.yard.common.event.aggregation;

import java.util.List;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

public interface SerializableListenerEventAggregator {

  List<SerializableListenerEvent> aggregate(List<SerializableListenerEvent> events);

}
