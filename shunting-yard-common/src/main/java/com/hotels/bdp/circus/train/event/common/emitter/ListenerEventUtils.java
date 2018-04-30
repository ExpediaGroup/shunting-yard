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
package com.hotels.bdp.circus.train.event.common.emitter;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.HiveAlterHandler;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

public final class ListenerEventUtils {

  private ListenerEventUtils() {}

  /**
   * This code is borrowed from {@link HiveAlterHandler}.
   *
   * @param event Listener event
   * @return {@code true} if the event refers to a CASCADE command, {@code false} otherwise.
   */
  public static boolean isCascade(ListenerEvent event) {
    final boolean cascade = event.getEnvironmentContext() != null
        && event.getEnvironmentContext().isSetProperties()
        && StatsSetupConst.TRUE.equals(event.getEnvironmentContext().getProperties().get(StatsSetupConst.CASCADE));
    return cascade;
  }

}
