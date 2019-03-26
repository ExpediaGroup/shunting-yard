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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;

public class ShuntingYardReplicationsTest {

  private static final String SOURCE_DATABASE = "db";
  private static final String SOURCE_TABLE = "tbl";
  private static final String REPLICA_DATABASE = "replica_db";
  private static final String REPLICA_TABLE = "replica_tbl";

  private ShuntingYardTableReplications syTableReplications;

  @Before
  public void init() {
    TableReplication tableReplication = new TableReplication();

    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(SOURCE_DATABASE);
    sourceTable.setTableName(SOURCE_TABLE);
    tableReplication.setSourceTable(sourceTable);

    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(REPLICA_DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);
    tableReplication.setReplicaTable(replicaTable);

    List<TableReplication> tableReplications = new ArrayList<>();
    tableReplications.add(tableReplication);

    TableReplications tableReplicationsWrapper = new CircusTrainTableReplications();
    tableReplicationsWrapper.setTableReplications(tableReplications);
    syTableReplications = new ShuntingYardTableReplications(tableReplicationsWrapper);
  }

  @Test
  public void typical() {
    Map<String, TableReplication> tableReplicationsMap = syTableReplications.getTableReplicationsMap();
    String key = String.join(".", SOURCE_DATABASE, SOURCE_TABLE);

    assertThat(tableReplicationsMap.get(key).getReplicaDatabaseName()).isEqualTo(REPLICA_DATABASE);
    assertThat(tableReplicationsMap.get(key).getReplicaTableName()).isEqualTo(REPLICA_TABLE);
  }

  @Test
  public void defaultConstructor() {
    syTableReplications = new ShuntingYardTableReplications();
    assertThat(syTableReplications.getTableReplicationsMap()).isEmpty();
  }

  @Test
  public void emptyTableReplications() {
    syTableReplications = new ShuntingYardTableReplications(new TableReplications());
    assertThat(syTableReplications.getTableReplicationsMap()).isEmpty();
  }

  @Test
  public void nullTableReplications() {
    syTableReplications = new ShuntingYardTableReplications(null);
    assertThat(syTableReplications.getTableReplicationsMap()).isEmpty();
  }

}
