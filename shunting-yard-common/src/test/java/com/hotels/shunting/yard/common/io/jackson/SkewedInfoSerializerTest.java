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
package com.hotels.shunting.yard.common.io.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.common.io.jackson.TestUtils.toJson;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;

public class SkewedInfoSerializerTest {

  private final SkewedInfo skewedInfo = new SkewedInfo();
  private ObjectMapper mapper;

  @Before
  public void init() {
    SimpleModule module = new SimpleModule();
    module.addSerializer(new SkewedInfoSerializer());
    mapper = new ObjectMapper();
    mapper.registerModule(module);
  }

  @Test
  public void full() throws Exception {
    skewedInfo.setSkewedColNames(Arrays.asList("col1", "col2"));
    skewedInfo.setSkewedColValues(Arrays.asList(Arrays.asList("val11", "val12"), Arrays.asList("val21", "val22")));
    skewedInfo.setSkewedColValueLocationMaps(ImmutableMap
        .<List<String>, String> builder()
        .put(Arrays.asList("10", "100"), "a")
        .put(Arrays.asList("20", "200"), "b")
        .build());
    String json = mapper.writeValueAsString(skewedInfo);
    assertThat(json).isEqualTo(toJson(skewedInfo));
  }

  @Test
  public void missingSkewedColNames() throws Exception {
    skewedInfo.setSkewedColValues(Arrays.asList(Arrays.asList("val11", "val12"), Arrays.asList("val21", "val22")));
    skewedInfo.setSkewedColValueLocationMaps(ImmutableMap
        .<List<String>, String> builder()
        .put(Arrays.asList("10", "100"), "a")
        .put(Arrays.asList("20", "200"), "b")
        .build());
    String json = mapper.writeValueAsString(skewedInfo);
    assertThat(json).isEqualTo(toJson(skewedInfo));
  }

  @Test
  public void missingSkewedColValues() throws Exception {
    skewedInfo.setSkewedColNames(Arrays.asList("col1", "col2"));
    skewedInfo.setSkewedColValueLocationMaps(ImmutableMap
        .<List<String>, String> builder()
        .put(Arrays.asList("10", "100"), "a")
        .put(Arrays.asList("20", "200"), "b")
        .build());
    String json = mapper.writeValueAsString(skewedInfo);
    assertThat(json).isEqualTo(toJson(skewedInfo));
  }

  @Test
  public void missingSkewedColValueLocationMaps() throws Exception {
    skewedInfo.setSkewedColNames(Arrays.asList("col1", "col2"));
    skewedInfo.setSkewedColValues(Arrays.asList(Arrays.asList("val11", "val12"), Arrays.asList("val21", "val22")));
    String json = mapper.writeValueAsString(skewedInfo);
    assertThat(json).isEqualTo(toJson(skewedInfo));
  }

}
