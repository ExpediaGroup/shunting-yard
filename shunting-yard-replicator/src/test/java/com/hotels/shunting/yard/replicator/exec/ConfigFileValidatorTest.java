/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.exec;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Joiner;

public class ConfigFileValidatorTest {

  private static final Joiner COMMA_JOINER = Joiner.on(",");

  public @Rule TemporaryFolder tmp = new TemporaryFolder();

  @Test(expected = ConfigFileValidationException.class)
  public void configFileLocationIsNotSet() throws Exception {
    ConfigFileValidator.validate(null);
  }

  @Test(expected = ConfigFileValidationException.class)
  public void configFileLocationIsBlank() throws Exception {
    ConfigFileValidator.validate("  ");
  }

  @Test
  public void singleFileExists() throws Exception {
    File location = tmp.newFile();
    ConfigFileValidator.validate(location.getAbsolutePath());
  }

  @Test(expected = ConfigFileValidationException.class)
  public void singleFileDoesNotExist() throws Exception {
    File location = new File(tmp.getRoot(), "trick");
    ConfigFileValidator.validate(location.getAbsolutePath());
  }

  @Test(expected = ConfigFileValidationException.class)
  public void singleFileIsADirectory() throws Exception {
    ConfigFileValidator.validate(tmp.getRoot().getAbsolutePath());
  }

  @Test
  public void multipleFilesExist() throws Exception {
    File location1 = tmp.newFile();
    File location2 = tmp.newFile();
    ConfigFileValidator.validate(COMMA_JOINER.join(location1.getAbsolutePath(), location2.getAbsolutePath()));
  }

  @Test(expected = ConfigFileValidationException.class)
  public void multipleFilesAndOneDoesNotExist() throws Exception {
    File location1 = tmp.newFile();
    File location2 = new File(tmp.getRoot(), "trick");
    ConfigFileValidator.validate(COMMA_JOINER.join(location1.getAbsolutePath(), location2.getAbsolutePath()));
  }

  @Test(expected = ConfigFileValidationException.class)
  public void multipleFilesAndOneIsADirectory() throws Exception {
    File location1 = tmp.newFile();
    File location2 = tmp.getRoot();
    ConfigFileValidator.validate(COMMA_JOINER.join(location1.getAbsolutePath(), location2.getAbsolutePath()));
  }

}
