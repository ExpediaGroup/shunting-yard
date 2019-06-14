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
package com.expediagroup.shuntingyard.replicator.exec.receiver;

import java.util.List;

import com.expediagroup.shuntingyard.replicator.exec.conf.SourceTableFilter;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

public class TableSelector {

  private final List<String> tableNames;

  public TableSelector(SourceTableFilter targetReplication) {
    this.tableNames = targetReplication.getTableNames();
  }

  public boolean canProcess(ListenerEvent listenerEvent) {
    String tableNameToBeProcessed = listenerEvent.getDbName() + "." + listenerEvent.getTableName();
    return tableNames.contains(tableNameToBeProcessed);
  }

}
