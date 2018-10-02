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
package com.hotels.shunting.yard.replicator.exec.receiver;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class Context {

  private final String workspace;
  private final String configLocation;
  private final Map<String, String> environment;

  Context(String workspace, String configLocation) {
    this(workspace, configLocation, ImmutableMap.of());
  }

  Context(String workspace, String configLocation, Map<String, String> environment) {
    this.workspace = workspace;
    this.configLocation = configLocation;
    this.environment = environment;
  }

  public String getWorkspace() {
    return workspace;
  }

  public String getConfigLocation() {
    return configLocation;
  }

  public final Map<String, String> getEnvironment() {
    return environment;
  }

}
