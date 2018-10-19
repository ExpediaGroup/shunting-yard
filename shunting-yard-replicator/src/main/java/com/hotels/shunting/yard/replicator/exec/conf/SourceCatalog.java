package com.hotels.shunting.yard.replicator.exec.conf;

import org.hibernate.validator.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "source-catalog")
public class SourceCatalog {
  private @NotBlank String name;
  private @NotBlank String hiveMetastoreUris;

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
}
