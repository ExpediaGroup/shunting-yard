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
package com.hotels.shunting.yard.common.metrics;

import java.util.Optional;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetricsHelper {
  private static final Logger log = LoggerFactory.getLogger(HiveMetricsHelper.class);

  public static Optional<Long> incrementCounter(String name) {
    try {
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        return Optional.of(metrics.incrementCounter(name));
      }
    } catch (Exception e) {
      log.warn("Unable to increment counter {}", name, e);
    }
    return Optional.empty();
  }

}
