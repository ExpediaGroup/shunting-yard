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
package com.hotels.shunting.yard.replicator.exec.context;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.base.Preconditions.checkNotNull;

import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.CT_CONFIG;
import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.WORKSPACE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.common.messaging.JsonMetaStoreEventDeserializer;
import com.expedia.apiary.extensions.receiver.common.messaging.MetaStoreEventDeserializer;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.shunting.yard.common.messaging.MessageReaderFactory;
import com.hotels.shunting.yard.replicator.exec.ConfigFileValidator;
import com.hotels.shunting.yard.replicator.exec.conf.EventReceiverConfiguration;
import com.hotels.shunting.yard.replicator.exec.conf.ReplicaCatalog;
import com.hotels.shunting.yard.replicator.exec.conf.SourceCatalog;
import com.hotels.shunting.yard.replicator.exec.conf.SourceTableFilter;
import com.hotels.shunting.yard.replicator.exec.event.aggregation.DefaultMetaStoreEventAggregator;
import com.hotels.shunting.yard.replicator.exec.event.aggregation.MetaStoreEventAggregator;
import com.hotels.shunting.yard.replicator.exec.external.Marshaller;
import com.hotels.shunting.yard.replicator.exec.launcher.CircusTrainRunner;
import com.hotels.shunting.yard.replicator.exec.messaging.AggregatingMetaStoreEventReader;
import com.hotels.shunting.yard.replicator.exec.messaging.FilteringMessageReader;
import com.hotels.shunting.yard.replicator.exec.messaging.MessageReaderAdapter;
import com.hotels.shunting.yard.replicator.exec.messaging.MetaStoreEventReader;
import com.hotels.shunting.yard.replicator.exec.receiver.CircusTrainReplicationMetaStoreEventListener;
import com.hotels.shunting.yard.replicator.exec.receiver.ContextFactory;
import com.hotels.shunting.yard.replicator.exec.receiver.ReplicationMetaStoreEventListener;
import com.hotels.shunting.yard.replicator.exec.receiver.TableSelector;
import com.hotels.shunting.yard.replicator.metastore.DefaultMetaStoreClientSupplier;

@Order(Ordered.HIGHEST_PRECEDENCE)
@org.springframework.context.annotation.Configuration
public class CommonBeans {
  private static final Logger LOG = LoggerFactory.getLogger(CommonBeans.class);

  @Bean
  Configuration baseConfiguration(
      @Value("${instance.workspace}") String workspace,
      @Value("${ct-config:#{null}}") String circusTrainConfigLocation) {
    checkNotNull(workspace, "instance.workspace is required");
    Configuration baseConf = new Configuration();
    baseConf.set(WORKSPACE.key(), workspace);

    if (circusTrainConfigLocation != null) {
      ConfigFileValidator.validate(circusTrainConfigLocation);
      baseConf.set(CT_CONFIG.key(), circusTrainConfigLocation);
    }
    return baseConf;
  }

  @Bean
  HiveConf replicaHiveConf(
      Configuration baseConfiguration,
      ReplicaCatalog replicaCatalog,
      EventReceiverConfiguration messageReaderConfig) {
    List<String> siteXml = replicaCatalog.getSiteXml();
    if (CollectionUtils.isEmpty(siteXml)) {
      LOG.info("No Hadoop site XML is defined for catalog {}.", replicaCatalog.getName());
    }
    Map<String, String> properties = new HashMap<>();
    for (Entry<String, String> entry : baseConfiguration) {
      properties.put(entry.getKey(), entry.getValue());
    }
    if (replicaCatalog.getHiveMetastoreUris() != null) {
      properties.put(ConfVars.METASTOREURIS.varname, replicaCatalog.getHiveMetastoreUris());
    }
    putConfigurationProperties(replicaCatalog.getConfigurationProperties(), properties);
    putConfigurationProperties(messageReaderConfig.getConfigurationProperties(), properties);
    HiveConf hiveConf = new HiveConfFactory(siteXml, properties).newInstance();
    return hiveConf;
  }

  private void putConfigurationProperties(Map<String, String> configurationProperties, Map<String, String> properties) {
    if (configurationProperties != null) {
      properties.putAll(configurationProperties);
    }
  }

  @Bean
  MetaStoreClientFactory thriftMetaStoreClientFactory() {
    return new CloseableMetaStoreClientFactory();
  }

  @Bean
  Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier(
      HiveConf replicaHiveConf,
      MetaStoreClientFactory replicaMetaStoreClientFactory) {
    return new DefaultMetaStoreClientSupplier(replicaHiveConf, replicaMetaStoreClientFactory);
  }

  @Bean
  ReplicationMetaStoreEventListener replicationMetaStoreEventListener(
      HiveConf replicaHiveConf,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    CloseableMetaStoreClient metaStoreClient = replicaMetaStoreClientSupplier.get();
    ContextFactory contextFactory = new ContextFactory(replicaHiveConf, metaStoreClient, new Marshaller());
    return new CircusTrainReplicationMetaStoreEventListener(metaStoreClient, contextFactory, new CircusTrainRunner());
  }

  @Bean
  MetaStoreEventAggregator eventAggregator() {
    return new DefaultMetaStoreEventAggregator();
  }

  @Bean
  TableSelector tableSelector(SourceTableFilter targetReplication) {
    return new TableSelector(targetReplication);
  }

  @Bean
  MessageReaderAdapter messageReaderAdapter(
      HiveConf replicaHiveConf,
      EventReceiverConfiguration messageReaderConfig,
      SourceCatalog sourceCatalog,
      TableSelector tableSelector) {
    MessageReaderFactory messaReaderFactory = MessageReaderFactory
        .newInstance(messageReaderConfig.getMessageReaderFactoryClass());
    MessageReader messageReader = messaReaderFactory.newInstance(replicaHiveConf);
    FilteringMessageReader filteringMessageReader = new FilteringMessageReader(messageReader, tableSelector);
    return new MessageReaderAdapter(filteringMessageReader, sourceCatalog.getHiveMetastoreUris());
  }

  @Bean
  MetaStoreEventReader eventReader(MessageReaderAdapter messageReaderAdapter) {
    return new AggregatingMetaStoreEventReader(messageReaderAdapter, eventAggregator());
  }

}
