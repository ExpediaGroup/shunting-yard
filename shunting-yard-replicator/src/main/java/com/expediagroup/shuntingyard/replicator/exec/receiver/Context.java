/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.replicator.exec.receiver;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;

public class Context {

  private final String workspace;
  private final String configLocation;
  private final String circusTrainConfigLocation;
  private final OrphanedDataStrategy orphanedDataStrategy;

  Context(String workspace, String configLocation, String circusTrainConfigLocation,
    OrphanedDataStrategy orphanedDataStrategy) {
    this.workspace = workspace;
    this.configLocation = configLocation;
    this.circusTrainConfigLocation = circusTrainConfigLocation;
    this.orphanedDataStrategy = orphanedDataStrategy;
  }

  public String getWorkspace() {
    return workspace;
  }

  public String getConfigLocation() {
    return configLocation;
  }

  public String getCircusTrainConfigLocation() {
    return circusTrainConfigLocation;
  }

  public OrphanedDataStrategy getOrphanedDataStrategy() {
    return orphanedDataStrategy;
  }

}
