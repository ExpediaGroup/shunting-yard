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

import java.util.List;
import java.util.Map;

import org.hibernate.validator.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "replica-catalog")
public class ReplicaCatalog {

  private @NotBlank String name;
  private @NotBlank String hiveMetastoreUris;
  private List<String> siteXml;
  private Map<String, String> configurationProperties;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHiveMetastoreUris() {
    return hiveMetastoreUris;
  }

  public void setHiveMetastoreUris(String hiveMetastoreUris) {
    this.hiveMetastoreUris = hiveMetastoreUris;
  }

  public List<String> getSiteXml() {
    return siteXml;
  }

  public void setSiteXml(List<String> siteXml) {
    this.siteXml = siteXml;
  }

  public Map<String, String> getConfigurationProperties() {
    return configurationProperties;
  }

  public void setConfigurationProperties(Map<String, String> configurationProperties) {
    this.configurationProperties = configurationProperties;
  }

}
