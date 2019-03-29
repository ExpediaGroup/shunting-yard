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

import com.hotels.shunting.yard.replicator.exec.conf.ct.SyReplicaTable;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SySourceTable;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplication;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplications;

public class ShuntingYardReplicationsTest {

  private static final String SOURCE_DATABASE = "db";
  private static final String SOURCE_TABLE = "tbl";
  private static final String REPLICA_DATABASE = "replica_db";
  private static final String REPLICA_TABLE = "replica_tbl";

  private ShuntingYardTableReplications syTableReplications;

  @Before
  public void init() {
    SyTableReplication tableReplication = new SyTableReplication();

    SySourceTable sourceTable = new SySourceTable();
    sourceTable.setDatabaseName(SOURCE_DATABASE);
    sourceTable.setTableName(SOURCE_TABLE);
    tableReplication.setSourceTable(sourceTable);

    SyReplicaTable replicaTable = new SyReplicaTable();
    replicaTable.setDatabaseName(REPLICA_DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);
    tableReplication.setReplicaTable(replicaTable);

    List<SyTableReplication> tableReplications = new ArrayList<>();
    tableReplications.add(tableReplication);

    SyTableReplications tableReplicationsWrapper = new SyTableReplications();
    tableReplicationsWrapper.setTableReplications(tableReplications);
    syTableReplications = new ShuntingYardTableReplications(tableReplicationsWrapper);
  }

  @Test
  public void typical() {
    SyTableReplication tableReplication = syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE);

    assertThat(tableReplication.getReplicaDatabaseName()).isEqualTo(REPLICA_DATABASE);
    assertThat(tableReplication.getReplicaTableName()).isEqualTo(REPLICA_TABLE);
  }

  @Test
  public void defaultConstructor() {
    syTableReplications = new ShuntingYardTableReplications();
    assertThat(syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE)).isNull();
  }

  @Test
  public void emptyTableReplications() {
    syTableReplications = new ShuntingYardTableReplications(new SyTableReplications());
    assertThat(syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE)).isNull();
  }

  @Test
  public void nullTableReplications() {
    syTableReplications = new ShuntingYardTableReplications(null);
    assertThat(syTableReplications.getTableReplication(SOURCE_DATABASE, SOURCE_TABLE)).isNull();
  }

}
