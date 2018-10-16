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
package com.hotels.shunting.yard.common.event;

import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAddPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryCreateTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryInsertTableEvent;

/**
 * To make processing event in the receiver easier.
 */
public enum EventType {
  ON_CREATE_TABLE(SerializableCreateTableEvent.class, false),
  ON_ALTER_TABLE(SerializableAlterTableEvent.class, false),
  ON_DROP_TABLE(SerializableDropTableEvent.class, false),
  ON_ADD_PARTITION(SerializableAddPartitionEvent.class, false),
  ON_ALTER_PARTITION(SerializableAlterPartitionEvent.class, false),
  ON_DROP_PARTITION(SerializableDropPartitionEvent.class, false),
  ON_INSERT(SerializableInsertEvent.class, false),
  ADD_PARTITION(SerializableApiaryAddPartitionEvent.class, true),
  ALTER_PARTITION(SerializableApiaryAlterPartitionEvent.class, true),
  DROP_PARTITION(SerializableApiaryDropPartitionEvent.class, true),
  CREATE_TABLE(SerializableApiaryCreateTableEvent.class, true),
  INSERT(SerializableApiaryInsertTableEvent.class, true),
  DROP_TABLE(SerializableApiaryDropTableEvent.class, true);

  private final Class<? extends SerializableListenerEvent> eventClass;
  private final boolean isApiaryEvent;

  private EventType(Class<? extends SerializableListenerEvent> eventClass, boolean isApiaryEvent) {
    if (eventClass == null) {
      throw new NullPointerException("Parameter eventClass is required");
    }
    this.eventClass = eventClass;
    this.isApiaryEvent = isApiaryEvent;
  }

  public Class<? extends SerializableListenerEvent> eventClass() {
    return eventClass;
  }

  public boolean isApiaryEvent() {
    return isApiaryEvent;
  }

  public static EventType forClass(Class<? extends SerializableListenerEvent> clazz) {
    for (EventType e : values()) {
      if (e.eventClass().equals(clazz)) {
        return e;
      }
    }
    throw new IllegalArgumentException("EventType not found for class " + clazz.getName());
  }

}
