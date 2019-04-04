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
package com.hotels.shunting.yard.replicator.exec.conf.ct;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.Before;
import org.junit.Test;

import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.SourceTable;

public class ShuntingYardTableReplicationTest {
  private SourceTable sourceTable;
  private ReplicaTable replicaTable;
  private ShuntingYardTableReplication tableReplication;
  private Validator validator;

  @Before
  public void before() {
    ValidatorFactory config = Validation.buildDefaultValidatorFactory();
    validator = config.getValidator();
  }

  @Before
  public void buildConfig() {
    sourceTable = new SourceTable();
    replicaTable = new ReplicaTable();

    tableReplication = new ShuntingYardTableReplication();
    tableReplication.setSourceTable(sourceTable);
    tableReplication.setReplicaTable(replicaTable);
  }

  @Test
  public void typical() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(0));
  }

  @Test
  public void typicalGetReplicaDatabaseAndTableName() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");

    assertThat(tableReplication.getReplicaDatabaseName(), is("replica-database"));
    assertThat(tableReplication.getReplicaTableName(), is("replica-table"));
  }

  @Test
  public void testGetReplicaDatabaseAndTableNameWhenSetAsUpperCase() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable.setDatabaseName("REPLICA-DATABASE");
    replicaTable.setTableName("REPLICA-TABLE");

    assertThat(tableReplication.getReplicaDatabaseName(), is("replica-database"));
    assertThat(tableReplication.getReplicaTableName(), is("replica-table"));
  }

  @Test
  public void testGetReplicaDatabaseAndTableNameWhenValueNotSet() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");

    assertThat(tableReplication.getReplicaDatabaseName(), is("source-database"));
    assertThat(tableReplication.getReplicaTableName(), is("source-table"));
  }

  @Test
  public void nullReplicaTable() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");

    tableReplication.setReplicaTable(null);

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullSourceTable() throws Exception {
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");

    tableReplication.setSourceTable(null);

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullSourceAndReplicaTables() throws Exception {
    tableReplication.setSourceTable(null);
    tableReplication.setReplicaTable(null);

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(2));
  }

  @Test
  public void sourceDbNotProvided() throws Exception {
    sourceTable.setTableName("source-table");
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void sourceTableNotProvided() throws Exception {
    sourceTable.setDatabaseName("source-database");
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void replicaDbNotProvided() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable.setTableName("replica-table");

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(0));
  }

  @Test
  public void replicaTableNotProvided() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable.setDatabaseName("replica-database");

    Set<ConstraintViolation<ShuntingYardTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(0));
  }

}
