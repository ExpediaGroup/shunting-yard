package com.hotels.shunting.yard.replicator.util;

public class TableDatabaseNameJoiner {

  public static String dotJoin(String x, String y) {
    return String.join(".", x, y);
  }

}
