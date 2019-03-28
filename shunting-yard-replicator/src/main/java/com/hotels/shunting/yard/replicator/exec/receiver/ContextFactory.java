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
package com.hotels.shunting.yard.replicator.exec.receiver;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.CT_CONFIG;
import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.WORKSPACE;

import java.io.File;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.PropertyUtils;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.replicator.exec.conf.ShuntingYardTableReplications;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.external.CircusTrainConfig;
import com.hotels.shunting.yard.replicator.exec.external.Marshaller;

public class ContextFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ContextFactory.class);

  private static final String TIMESTAMP_FORMAT = "yyyyMMdd'T'HHmmssSSS";
  private static final String TABLE_LOCATION_REGEX = "(.*)/(ct\\w-\\d{8}t\\d{6}\\.\\d{3}z-\\w{8}/?)";
  private static final Pattern TABLE_LOCATION_PATTERN = Pattern.compile(TABLE_LOCATION_REGEX);

  private final Configuration conf;
  private final CloseableMetaStoreClient metaStoreClient;
  private final Marshaller marshaller;
  private final ShuntingYardTableReplications shuntingYardTableReplications;

  public ContextFactory(
      Configuration conf,
      CloseableMetaStoreClient metaStoreClient,
      Marshaller marshaller,
      ShuntingYardTableReplications shuntingYardTableReplications) {
    this.conf = conf;
    this.metaStoreClient = metaStoreClient;
    this.marshaller = marshaller;
    this.shuntingYardTableReplications = shuntingYardTableReplications;
  }

  private String dir(MetaStoreEvent event) {
    String timestamp = DateTimeFormat.forPattern(TIMESTAMP_FORMAT).print(new DateTime());
    return event.getEventType().name() + "_" + timestamp;
  }

  private String getSourceMetaStoreUri(MetaStoreEvent event) {
    return event.getParameters().get(METASTOREURIS.varname);
  }

  private String workOutReplicaLocation(String databaseName, String tableName) {
    try {
      try {
        Table replicaTable = metaStoreClient.getTable(databaseName, tableName);
        String tableLocation = replicaTable.getSd().getLocation();
        LOG.info("Replica table current location is {}", tableLocation);
        // If the table has been replicated before then remove the previous event ID
        Matcher matcher = TABLE_LOCATION_PATTERN.matcher(tableLocation);
        if (matcher.matches()) {
          tableLocation = matcher.group(1);
        }
        LOG.info("Replica table location will be {}", tableLocation);
        return tableLocation;
      } catch (NoSuchObjectException e) {
        // Ignore and work the location out using the DB location and table name
      }
      Database db = metaStoreClient.getDatabase(databaseName);
      URI tableLocation = URI.create(db.getLocationUri() + "/" + tableName);
      return tableLocation.toString();
    } catch (TException e) {
      throw new ShuntingYardException("Unable to work replica location out", e);
    }
  }

  public Context createContext(MetaStoreEvent event) {
    String circusTrainConfigLocation = PropertyUtils.stringProperty(conf, CT_CONFIG);
    File workspace = new File(PropertyUtils.stringProperty(conf, WORKSPACE), dir(event));
    workspace.mkdirs();
    File configLocation = new File(workspace, "replication.yml");

    CircusTrainConfig circusTrainConfig = generateConfiguration(event);

    marshaller.marshall(configLocation.getAbsolutePath(), circusTrainConfig);

    return new Context(workspace.getAbsolutePath(), configLocation.getAbsolutePath(), circusTrainConfigLocation);
  }

  private CircusTrainConfig generateConfiguration(MetaStoreEvent event) {
    String sourceMetaStoreUri = getSourceMetaStoreUri(event);

    String replicaDatabaseName = event.getDatabaseName();
    String replicaTableName = event.getTableName();

    TableReplication tableReplication = shuntingYardTableReplications
        .getTableReplication(event.getDatabaseName(), event.getTableName());

    if (tableReplication != null) {
      replicaDatabaseName = tableReplication.getReplicaDatabaseName();
      replicaTableName = tableReplication.getReplicaTableName();
    }

    String replicaTableLocation = workOutReplicaLocation(replicaDatabaseName, replicaTableName);
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceMetaStoreUri(sourceMetaStoreUri)
        .replicaMetaStoreUri(conf.get(METASTOREURIS.varname))
        .replication(event.getReplicationMode(), event.getDatabaseName(), event.getTableName(), replicaDatabaseName,
            replicaTableName, replicaTableLocation, event.getPartitionColumns(), event.getPartitionValues())
        .build();
    return config;
  }

}
