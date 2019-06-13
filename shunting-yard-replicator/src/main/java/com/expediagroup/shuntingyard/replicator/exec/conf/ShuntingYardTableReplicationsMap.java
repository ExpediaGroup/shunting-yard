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
package com.expediagroup.shuntingyard.replicator.exec.conf;

import java.util.HashMap;
import java.util.Map;

import com.expediagroup.shuntingyard.replicator.exec.conf.ct.ShuntingYardTableReplication;
import com.expediagroup.shuntingyard.replicator.exec.conf.ct.ShuntingYardTableReplications;
import com.expediagroup.shuntingyard.replicator.util.TableDatabaseNameJoiner;

public class ShuntingYardTableReplicationsMap {
  private final Map<String, ShuntingYardTableReplication> tableReplicationsMap = new HashMap<>();

  public ShuntingYardTableReplicationsMap(ShuntingYardTableReplications tableReplications) {
    if ((tableReplications != null) && (tableReplications.getTableReplications() != null)) {
      for (ShuntingYardTableReplication tableReplication : tableReplications.getTableReplications()) {
        String key = TableDatabaseNameJoiner
            .dotJoin(tableReplication.getSourceTable().getDatabaseName().toLowerCase(),
                tableReplication.getSourceTable().getTableName().toLowerCase());
        tableReplicationsMap.put(key, tableReplication);
      }
    }
  }

  public ShuntingYardTableReplication getTableReplication(String dbName, String tableName) {
    String key = TableDatabaseNameJoiner.dotJoin(dbName.toLowerCase(), tableName.toLowerCase());
    return tableReplicationsMap.get(key);
  }

}
