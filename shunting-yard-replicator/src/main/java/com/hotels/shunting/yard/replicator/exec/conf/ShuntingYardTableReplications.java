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
package com.hotels.shunting.yard.replicator.exec.conf;

import java.util.HashMap;
import java.util.Map;

import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplication;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplications;

public class ShuntingYardTableReplications {
  private final Map<String, SyTableReplication> tableReplicationsMap = new HashMap<>();

  public ShuntingYardTableReplications() {}

  public ShuntingYardTableReplications(SyTableReplications tableReplications) {
    if ((tableReplications != null) && (tableReplications.getTableReplications() != null)) {
      for (SyTableReplication tableReplication : tableReplications.getTableReplications()) {
        String key = String
            .join(".", tableReplication.getSourceTable().getDatabaseName(),
                tableReplication.getSourceTable().getTableName());
        tableReplicationsMap.put(key, tableReplication);
      }
    }
  }

  public SyTableReplication getTableReplication(String dbName, String tableName) {
    String key = String.join(".", dbName, tableName);
    return tableReplicationsMap.get(key);
  }

}
