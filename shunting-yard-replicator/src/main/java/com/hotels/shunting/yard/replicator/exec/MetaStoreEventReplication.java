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
package com.hotels.shunting.yard.replicator.exec;

import static com.hotels.shunting.yard.replicator.exec.Constants.CIRCUS_TRAIN_HOME_ENV_VAR;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.environment.EnvironmentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.validation.BindException;
import org.springframework.validation.ObjectError;

import com.codahale.metrics.MetricRegistry;

@SpringBootApplication
@EnableConfigurationProperties
public class MetaStoreEventReplication {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreEventReplication.class);

  public static void main(String[] args) throws Exception {
    // below is output *before* logging is configured so will appear on console
    logVersionInfo();

    verifyCircusTrainInstallation();

    int exitCode = -1;
    try {
      exitCode = SpringApplication
          .exit(new SpringApplicationBuilder(MetaStoreEventReplication.class)
              .properties("spring.config.location:${config:null},${ct-config:null}")
              .properties("instance.home:${user.home}")
              .properties("instance.name:${replica-catalog.name}")
              .properties("instance.workspace:${instance.home}/.shunting-yard")
              .registerShutdownHook(true)
              .listeners(new ConfigFileValidationApplicationListener())
              .build()
              .run(args));
    } catch (ConfigFileValidationException e) {
      LOG.error(e.getMessage(), e);
      printHelp(e.getErrors());
    } catch (BeanCreationException e) {
      LOG.error(e.getMessage(), e);
      if (e.getMostSpecificCause() instanceof BindException) {
        printHelp(((BindException) e.getMostSpecificCause()).getAllErrors());
      }
    }

    System.exit(exitCode);
  }

  private static void verifyCircusTrainInstallation() throws IOException {
    Map<String, String> env = EnvironmentUtils.getProcEnvironment();
    String circusTrainHome = env.get(CIRCUS_TRAIN_HOME_ENV_VAR);
    if (circusTrainHome == null || circusTrainHome.isEmpty()) {
      throw new RuntimeException("The enviroment variable " + CIRCUS_TRAIN_HOME_ENV_VAR + " is not set");
    }

  }

  private static void printHelp(List<ObjectError> allErrors) {
    System.out.println(new MetaStoreEventReplicationHelp(allErrors));
  }

  MetaStoreEventReplication() {
    // below is output *after* logging is configured so will appear in log file
    logVersionInfo();
  }

  private static void logVersionInfo() {
    // ManifestAttributes manifestAttributes = new ManifestAttributes(CircusTrain.class);
    // LOG.info("{}", manifestAttributes);
    LOG.info("MetaStoreEventReplication");
  }

  @Bean
  MetricRegistry runningMetricRegistry() {
    return new MetricRegistry();
  }

}
