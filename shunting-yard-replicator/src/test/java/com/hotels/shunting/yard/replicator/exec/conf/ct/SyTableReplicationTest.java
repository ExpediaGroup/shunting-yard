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

public class SyTableReplicationTest {
  private SySourceTable sourceTable;
  private SyReplicaTable replicaTable;
  private SyTableReplication tableReplication;
  private Validator validator;

  @Before
  public void before() {
    ValidatorFactory config = Validation.buildDefaultValidatorFactory();
    validator = config.getValidator();
  }

  @Before
  public void buildConfig() {
    sourceTable = new SySourceTable();
    replicaTable = new SyReplicaTable();

    tableReplication = new SyTableReplication();
    tableReplication.setSourceTable(sourceTable);
    tableReplication.setReplicaTable(replicaTable);
  }

  @Test
  public void typical() throws Exception {
    sourceTable.setDatabaseName("source-database");
    sourceTable.setTableName("source-table");
    replicaTable.setDatabaseName("replica-database");
    replicaTable.setTableName("replica-table");

    Set<ConstraintViolation<SyTableReplication>> violations = validator.validate(tableReplication);

    assertThat(violations.size(), is(0));
  }

  // @Test
  // public void nullTableLocation() throws Exception {
  // replicaTable.setTableLocation(null);
  //
  // Set<ConstraintViolation<TableReplication>> violations = validator.validate(tableReplication);
  //
  // assertThat(violations.size(), is(1));
  // }
}
