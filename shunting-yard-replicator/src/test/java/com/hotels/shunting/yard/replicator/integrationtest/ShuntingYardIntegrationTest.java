/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.integrationtest;

import static com.hotels.shunting.yard.replicator.integrationtest.IntegrationTestHelper.DATABASE;
import static com.hotels.shunting.yard.replicator.integrationtest.cthelpers.TestUtils.toUri;

import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.google.common.base.Supplier;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.external.Marshaller;
import com.hotels.shunting.yard.replicator.exec.launcher.CircusTrainRunner;
import com.hotels.shunting.yard.replicator.exec.receiver.CircusTrainReplicationMetaStoreEventListener;
import com.hotels.shunting.yard.replicator.exec.receiver.ContextFactory;
import com.hotels.shunting.yard.replicator.exec.receiver.ReplicationMetaStoreEventListener;
import com.hotels.shunting.yard.replicator.metastore.DefaultMetaStoreClientSupplier;

public class ShuntingYardIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ShuntingYardIntegrationTest.class);

  private static final String TARGET_UNPARTITIONED_TABLE = "ct_table_u_copy";
  private static final String TARGET_PARTITIONED_TABLE = "ct_table_p_copy";
  private static final String TARGET_UNPARTITIONED_MANAGED_TABLE = "ct_table_u_managed_copy";
  private static final String TARGET_PARTITIONED_MANAGED_TABLE = "ct_table_p_managed_copy";
  private static final String TARGET_PARTITIONED_VIEW = "ct_view_p_copy";
  private static final String TARGET_UNPARTITIONED_VIEW = "ct_view_u_copy";

  public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule DataFolder dataFolder = new ClassDataFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule sourceCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
  public @Rule ThriftHiveMetaStoreJUnitRule replicaCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);

  private File sourceWarehouseUri;
  private File replicaWarehouseUri;
  private File housekeepingDbLocation;

  private IntegrationTestHelper helper;

  @Before
  public void init() throws Exception {
    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    replicaWarehouseUri = temporaryFolder.newFolder("replica-warehouse");
    temporaryFolder.newFolder("db");
    housekeepingDbLocation = new File(new File(temporaryFolder.getRoot(), "db"), "housekeeping");

    helper = new IntegrationTestHelper(sourceCatalog.client());
  }

  @Test
  public void partitionedTableHousekeepingEnabledNoAudit() throws Exception {
    helper.createPartitionedTable(toUri(sourceWarehouseUri, DATABASE, IntegrationTestHelper.SOURCE_PARTITIONED_TABLE));
    LOG
        .info(">>>> Table {} ",
            sourceCatalog.client().getTable(DATABASE, IntegrationTestHelper.SOURCE_PARTITIONED_TABLE));

    // exit.expectSystemExitWithStatus(0);
    // File config = dataFolder.getFile("partitioned-single-table-no-housekeeping.yml");

    // start

    HiveConf replicaHiveConf = replicaCatalog.conf();
    Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier = new DefaultMetaStoreClientSupplier(
        replicaHiveConf, new CloseableMetaStoreClientFactory());

    CloseableMetaStoreClient metaStoreClient = replicaMetaStoreClientSupplier.get();
    ContextFactory contextFactory = new ContextFactory(replicaHiveConf, metaStoreClient, new Marshaller());
    ReplicationMetaStoreEventListener eventListener = new CircusTrainReplicationMetaStoreEventListener(metaStoreClient,
        contextFactory, new CircusTrainRunner());
    sourceCatalog.conf().getAllProperties();
    EnvironmentContext sourceContext = new EnvironmentContext();
    // end

    // Map<String, String> map = new HashMap<>();
    //
    // sourceCatalog.conf().getAllProperties().putAll(map);

    Map<String, String> map = sourceCatalog
        .conf()
        .getAllProperties()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));

    System.out.println("HELLO WORLD - " + map.get(ConfVars.METASTOREURIS.varname) + "**");

    Map<String, String> env = EnvironmentUtils.getProcEnvironment();
    EnvironmentUtils
        .addVariableToEnvironment(env,
            "CIRCUS_TRAIN_HOME=/Users/abhgupta/Desktop/workspace/shunting-yard/circus-train-12.1.0");
   EnvironmentUtils.addVariableToEnvironment(env, "HIVE_LIB=/usr/local/opt/hive/lib");
    EnvironmentUtils
        .addVariableToEnvironment(env, "HCAT_LIB=/usr/local/opt/hive/hcatalog");
//    EnvironmentUtils
//        .addVariableToEnvironment(env, "HADOOP_HOME=/usr/local/opt/hadoop/");
//    EnvironmentUtils
//        .addVariableToEnvironment(env, "HADOOP_CLASSPATH=/usr/local/opt/hadoop/share/hadoop/tools/lib");

   // EnvironmentUtils.addVariableToEnvironment(env, "HADOOP_HOME=/usr/local/Cellar/hadoop/3.1.1/");
//    EnvironmentUtils
//        .addVariableToEnvironment(env, "HADOOP_CLASSPATH=$(find $HADOOP_HOME -name '*.jar' | xargs echo | tr ' ' ':')");

    // export HADOOP_COMMON_HOME=/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/

    MetaStoreEvent event = MetaStoreEvent
        .builder(EventType.ON_ADD_PARTITION, DATABASE, IntegrationTestHelper.SOURCE_PARTITIONED_TABLE)
        .parameters(map)
        .environmentContext(env)
        .build();

    System.out.println(env.get("CIRCUS_TRAIN_HOME"));
    System.out.println(env.get("HIVE_LIB"));
    System.out.println(env.get("HCAT_LIB"));
//    System.out.println(env.get("HADOOP_HOME"));
//    System.out.println(env.get("HADOOP_CLASSPATH"));

    eventListener.onEvent(event);

    // ReplicationRunner runner = new ReplicationRunner(eventReader, listener);

    // CircusTrainRunner runner = CircusTrainRunner
    // .builder(DATABASE, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation)
    // .sourceMetaStore(sourceCatalog.getThriftConnectionUri(), sourceCatalog.connectionURL(),
    // sourceCatalog.driverClassName())
    // .replicaMetaStore(replicaCatalog.getThriftConnectionUri())
    // .build();
    // exit.checkAssertionAfterwards(new Assertion() {
    // @Override
    // public void checkAssertion() throws Exception {
    // String jdbcUrl = housekeepingDbJdbcUrl();
    // try (Connection conn = getConnection(jdbcUrl, HOUSEKEEPING_DB_USER, HOUSEKEEPING_DB_PASSWD)) {
    // List<LegacyReplicaPath> cleanUpPaths = TestUtils
    // .getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path");
    // assertThat(cleanUpPaths.size(), is(0));
    // try {
    // getCleanUpPaths(conn, "SELECT * FROM circus_train.legacy_replica_path_aud");
    // } catch (SQLException e) {
    // assertThat(e.getMessage().startsWith("Table \"LEGACY_REPLICA_PATH_AUD\" not found;"), is(true));
    // }
    // }
    // }
    // });
    // runner.run(config.getAbsolutePath());
  }

}
