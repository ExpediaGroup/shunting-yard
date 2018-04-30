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
package com.hotels.bdp.circus.train.event.common.event;

import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;

public class SerializableCreateTableEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  private final Table table;

  public SerializableCreateTableEvent(CreateTableEvent event) {
    super(event);
    table = event.getTable();
  }

  @Override
  public EventType getEventType() {
    return EventType.ON_CREATE_TABLE;
  }

  @Override
  public String getDatabaseName() {
    return table.getDbName();
  }

  @Override
  public String getTableName() {
    return table.getTableName();
  }

  public Table getTable() {
    return table;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SerializableCreateTableEvent)) {
      return false;
    }
    SerializableCreateTableEvent other = (SerializableCreateTableEvent) obj;
    return super.equals(other) && Objects.equals(table, other.table);
  }

}
