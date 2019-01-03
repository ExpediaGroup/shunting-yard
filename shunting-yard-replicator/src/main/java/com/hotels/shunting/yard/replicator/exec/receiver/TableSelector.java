/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.exec.receiver;

import java.util.List;

import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.replicator.exec.conf.TargetReplication;

public class TableSelector {

  private final TargetReplication targetReplication;

  public TableSelector(TargetReplication targetReplication) {
    this.targetReplication = targetReplication;
  }

  public boolean canProcess(ListenerEvent listenerEvent) {
    List<String> tableNames = targetReplication.getTableNames();
    String tableNameToBeProcessed = listenerEvent.getDbName() + "." + listenerEvent.getTableName();

    return tableNames.contains(tableNameToBeProcessed);
  }

}
