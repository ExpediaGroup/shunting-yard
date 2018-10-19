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
package com.hotels.shunting.yard.common.event.apiary;

import org.apache.hadoop.hive.metastore.events.CreateTableEvent;

public class SerializableApiaryCreateTableEvent extends SerializableApiaryListenerEvent {
  private static final long serialVersionUID = 1L;

  private String protocolVersion;
  private String dbName;
  private String tableName;

  SerializableApiaryCreateTableEvent() {}

  public SerializableApiaryCreateTableEvent(CreateTableEvent event) {
    super(event);
    dbName = event.getTable().getDbName();
    tableName = event.getTable().getTableName();
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public String getDbName() {
    return dbName;
  }

  @Override
  public String getDatabaseName() {
    return dbName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }
}
