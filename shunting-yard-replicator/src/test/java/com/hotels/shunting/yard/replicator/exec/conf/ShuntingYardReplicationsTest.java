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

import org.junit.Before;
import org.junit.Test;

import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplication;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplications;

public class ShuntingYardReplicationsTest {

  private static final String SOURCE_DATABASE = "DATABASE";
  private static final String SOURCE_TABLE = "TABLE";
  private static final String REPLICA_DATABASE = "replica_db";
  private static final String REPLICA_TABLE = "replica_tbl";

  private ShuntingYardReplications syTableReplications;

  @Before
  public void init() {
    SyTableReplication tableReplication = new SyTableReplication();

    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(SOURCE_DATABASE);
    sourceTable.setTableName(SOURCE_TABLE);
    tableReplication.setSourceTable(sourceTable);

    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(REPLICA_DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);
    tableReplication.setReplicaTable(replicaTable);

    List<SyTableReplication> tableReplications = new ArrayList<>();
    tableReplications.add(tableReplication);

    SyTableReplications tableReplicationsWrapper = new SyTableReplications();
    tableReplicationsWrapper.setTableReplications(tableReplications);
    syTableReplications = new ShuntingYardReplications(tableReplicationsWrapper);
  }

  @Test
  public void typical() {
    SyTableReplication tableReplication = syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE);

    assertThat(tableReplication.getReplicaDatabaseName()).isEqualTo(REPLICA_DATABASE);
    assertThat(tableReplication.getReplicaTableName()).isEqualTo(REPLICA_TABLE);
  }

  @Test
  public void queryMapWithLowerCaseSourceDatabaseAndTable() {
    SyTableReplication tableReplication = syTableReplications
        .getTableReplication(SOURCE_DATABASE.toLowerCase(), SOURCE_TABLE.toLowerCase());

    assertThat(tableReplication.getReplicaDatabaseName()).isEqualTo(REPLICA_DATABASE);
    assertThat(tableReplication.getReplicaTableName()).isEqualTo(REPLICA_TABLE);
  }

  @Test
  public void defaultConstructor() {
    syTableReplications = new ShuntingYardReplications();
    assertThat(syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE)).isNull();
  }

  @Test
  public void emptyTableReplications() {
    syTableReplications = new ShuntingYardReplications(new SyTableReplications());
    assertThat(syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE)).isNull();
  }

  @Test
  public void nullTableReplications() {
    syTableReplications = new ShuntingYardReplications(null);
    assertThat(syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE)).isNull();
  }

}
