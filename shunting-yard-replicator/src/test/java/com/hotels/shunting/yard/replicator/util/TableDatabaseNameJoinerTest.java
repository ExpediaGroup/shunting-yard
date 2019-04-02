package com.hotels.shunting.yard.replicator.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TableDatabaseNameJoinerTest {

  @Test
  public void typical() {
    String databaseName = "database";
    String tableName = "table";

    assertThat(TableDatabaseNameJoiner.dotJoin(databaseName, tableName)).isEqualTo("database.table");
  }

  @Test
  public void nullInput() {
    String databaseName = "database";
    String tableName = null;

    assertThat(TableDatabaseNameJoiner.dotJoin(databaseName, tableName)).isEqualTo("database.null");
  }

  @Test
  public void blankInput() {
    String databaseName = "database";
    String tableName = "";

    assertThat(TableDatabaseNameJoiner.dotJoin(databaseName, tableName)).isEqualTo("database.");
  }

}
