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
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryCreateTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryInsertTableEvent;

/**
 * To make processing event in the receiver easier.
 */
public enum EventType {
  ADD_PARTITION(SerializableApiaryAddPartitionEvent.class),
  ALTER_PARTITION(SerializableApiaryAlterPartitionEvent.class),
  DROP_PARTITION(SerializableApiaryDropPartitionEvent.class),
  CREATE_TABLE(SerializableApiaryCreateTableEvent.class),
  INSERT(SerializableApiaryInsertTableEvent.class),
  DROP_TABLE(SerializableApiaryDropTableEvent.class),
  ALTER_TABLE(SerializableApiaryAlterTableEvent.class);

  private final Class<? extends SerializableListenerEvent> eventClass;

  private EventType(Class<? extends SerializableListenerEvent> eventClass) {
    if (eventClass == null) {
      throw new NullPointerException("Parameter eventClass is required");
    }
    this.eventClass = eventClass;
  }

  public Class<? extends SerializableListenerEvent> eventClass() {
    return eventClass;
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
