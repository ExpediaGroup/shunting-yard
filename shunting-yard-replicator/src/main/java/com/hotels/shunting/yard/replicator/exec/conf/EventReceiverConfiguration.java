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
package com.hotels.shunting.yard.replicator.exec.conf;

import static com.hotels.shunting.yard.replicator.exec.conf.SerDeType.JSON;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "event-receiver")
public class EventReceiverConfiguration {

  private Map<String, String> configurationProperties;
  private @NotNull String messageReaderClass;
  private @NotNull SerDeType serDeType = JSON;

  public Map<String, String> getConfigurationProperties() {
    return configurationProperties;
  }

  public void setConfigurationProperties(Map<String, String> configurationProperties) {
    this.configurationProperties = configurationProperties;
  }

  public String getMessageReaderClass() {
    return messageReaderClass;
  }

  public void setMessageReaderClass(String messageReaderClass) {
    this.messageReaderClass = messageReaderClass;
  }

  public SerDeType getSerDeType() {
    return serDeType;
  }

  public void setSerDeType(SerDeType serDeType) {
    this.serDeType = serDeType;
  }

}
