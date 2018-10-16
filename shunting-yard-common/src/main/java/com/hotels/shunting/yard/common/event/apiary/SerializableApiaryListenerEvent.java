package com.hotels.shunting.yard.common.event.apiary;

import javax.annotation.concurrent.NotThreadSafe;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

/**
 * @see {@link SerializableListenerEvent}
 */
@NotThreadSafe
public abstract class SerializableApiaryListenerEvent extends SerializableListenerEvent {
  private static final long serialVersionUID = 1L;

  /**
   * Thrift URI of the source Hive Metastore
   */
  public abstract String getSourceMetastoreUris();

  @Override
  public abstract String getDatabaseName();

  @Override
  public abstract String getTableName();

}
