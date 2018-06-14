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
package com.hotels.shunting.yard.common.emitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.shunting.yard.common.metrics.MetricsConstant;
import com.hotels.shunting.yard.common.metrics.MetricsHelper;

public final class EmitterUtils {
  private static final Logger log = LoggerFactory.getLogger(EmitterUtils.class);

  public static void success() {
    MetricsHelper.incrementCounter(MetricsConstant.EMITTER_SUCCESSES);
  }

  public static void error(Exception e) {
    // ERROR, ShuntingYard and Emitter are keywords
    log.error("Error in ShuntingYard Emitter", e);
    MetricsHelper.incrementCounter(MetricsConstant.EMITTER_FAILURES);
  }

}
