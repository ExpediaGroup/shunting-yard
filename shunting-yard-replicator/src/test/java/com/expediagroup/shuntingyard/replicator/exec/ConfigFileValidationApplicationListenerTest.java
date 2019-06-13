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
package com.expediagroup.shuntingyard.replicator.exec;

import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.core.env.ConfigurableEnvironment;

import com.expediagroup.shuntingyard.replicator.exec.ConfigFileValidationApplicationListener;
import com.expediagroup.shuntingyard.replicator.exec.ConfigFileValidationException;
import com.google.common.base.Joiner;

@RunWith(MockitoJUnitRunner.class)
public class ConfigFileValidationApplicationListenerTest {

  private static final String SPRING_CONFIG_LOCATION = "spring.config.location";
  private static final Joiner COMMA_JOINER = Joiner.on(",");

  public @Rule TemporaryFolder tmp = new TemporaryFolder();

  private @Mock ApplicationEnvironmentPreparedEvent event;
  private @Mock ConfigurableEnvironment environment;

  private final ConfigFileValidationApplicationListener listener = new ConfigFileValidationApplicationListener();

  @Before
  public void init() {
    when(event.getEnvironment()).thenReturn(environment);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void configFileLocationIsNotSet() throws Exception {
    listener.onApplicationEvent(event);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void configFileLocationIsBlank() throws Exception {
    when(environment.getProperty(SPRING_CONFIG_LOCATION)).thenReturn(" ");
    listener.onApplicationEvent(event);
  }

  @Test
  public void singleFileExists() throws Exception {
    File location = tmp.newFile();
    when(environment.getProperty(SPRING_CONFIG_LOCATION)).thenReturn(location.getAbsolutePath());
    listener.onApplicationEvent(event);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void singleFileDoesNotExist() throws Exception {
    File location = new File(tmp.getRoot(), "trick");
    when(environment.getProperty(SPRING_CONFIG_LOCATION)).thenReturn(location.getAbsolutePath());
    listener.onApplicationEvent(event);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void singleFileIsADirectory() throws Exception {
    when(environment.getProperty(SPRING_CONFIG_LOCATION)).thenReturn(tmp.getRoot().getAbsolutePath());
    listener.onApplicationEvent(event);
  }

  @Test
  public void multipleFilesExist() throws Exception {
    File location1 = tmp.newFile();
    File location2 = tmp.newFile();
    when(environment.getProperty(SPRING_CONFIG_LOCATION))
        .thenReturn(COMMA_JOINER.join(location1.getAbsolutePath(), location2.getAbsolutePath()));
    listener.onApplicationEvent(event);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void multipleFilesAndOneDoesNotExist() throws Exception {
    File location1 = tmp.newFile();
    File location2 = new File(tmp.getRoot(), "trick");
    when(environment.getProperty(SPRING_CONFIG_LOCATION))
        .thenReturn(COMMA_JOINER.join(location1.getAbsolutePath(), location2.getAbsolutePath()));
    listener.onApplicationEvent(event);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void multipleFilesAndOneIsADirectory() throws Exception {
    File location1 = tmp.newFile();
    File location2 = tmp.getRoot();
    when(environment.getProperty(SPRING_CONFIG_LOCATION))
        .thenReturn(COMMA_JOINER.join(location1.getAbsolutePath(), location2.getAbsolutePath()));
    listener.onApplicationEvent(event);
  }

}
