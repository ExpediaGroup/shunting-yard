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
package com.hotels.shunting.yard.replicator.exec.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

import com.hotels.shunting.yard.common.metrics.MetricsConstant;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.messaging.MetaStoreEventReader;
import com.hotels.shunting.yard.replicator.exec.receiver.ReplicationMetaStoreEventListener;

@Component
class ReplicationRunner implements ApplicationRunner, ExitCodeGenerator {
  private static final Logger log = LoggerFactory.getLogger(ReplicationRunner.class);

  private static final Counter SUCCESS_COUNTER = Metrics.counter(MetricsConstant.RECEIVER_SUCCESSES);
  private static final Counter FAILURE_COUNTER = Metrics.counter(MetricsConstant.RECEIVER_FAILURES);

  private final ReplicationMetaStoreEventListener listener;
  private final MetaStoreEventReader eventReader;

  @Autowired
  ReplicationRunner(MetaStoreEventReader eventReader, ReplicationMetaStoreEventListener listener) {
    this.listener = listener;
    this.eventReader = eventReader;
  }

  @Override
  public void run(ApplicationArguments args) {
    while (eventReader.hasNext()) {
      try {
        MetaStoreEvent event = eventReader.next();
        log.info("New event received: {}", event);
        listener.onEvent(event);
        SUCCESS_COUNTER.increment();
      } catch (Exception e) {
        // ERROR, ShuntingYard and Receiver are keywords
        log.error("Error in ShuntingYard Receiver", e);
        FAILURE_COUNTER.increment();
      }
    }
    log.info("Finishing event loop");
  }

  @Override
  public int getExitCode() {
    return 0;
  }

}
