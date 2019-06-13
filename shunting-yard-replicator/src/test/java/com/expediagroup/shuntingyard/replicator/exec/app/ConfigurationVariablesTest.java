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
package com.expediagroup.shuntingyard.replicator.exec.app;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.shuntingyard.replicator.exec.app.ConfigurationVariables.CT_CONFIG;
import static com.expediagroup.shuntingyard.replicator.exec.app.ConfigurationVariables.WORKSPACE;

import org.junit.Test;

import com.expediagroup.shuntingyard.replicator.exec.app.ConfigurationVariables;

public class ConfigurationVariablesTest {

  private static String prefixedKey(String key) {
    return "com.hotels.shunting.yard.replicator.exec.app." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(ConfigurationVariables.values().length).isEqualTo(2);
  }

  @Test
  public void workspace() {
    assertThat(WORKSPACE.unPrefixedKey()).isEqualTo("workspace");
    assertThat(WORKSPACE.key()).isEqualTo(prefixedKey("workspace"));
    assertThat(WORKSPACE.defaultValue()).isNull();
  }

  @Test
  public void circusTrainConfig() {
    assertThat(CT_CONFIG.unPrefixedKey()).isEqualTo("ct-config");
    assertThat(CT_CONFIG.key()).isEqualTo(prefixedKey("ct-config"));
    assertThat(CT_CONFIG.defaultValue()).isNull();
  }

}
