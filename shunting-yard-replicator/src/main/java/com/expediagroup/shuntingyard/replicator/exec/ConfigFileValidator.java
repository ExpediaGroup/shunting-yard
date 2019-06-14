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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;

public class ConfigFileValidator {
  public static void validate(String configFileLocation) {
    List<String> errors = new ArrayList<>();
    if (StringUtils.isEmpty(configFileLocation)) {
      errors.add("No config file was specified.");
    } else {
      for (String configFileString : Splitter.on(',').split(configFileLocation)) {
        File configFile = new File(configFileString);
        if (!configFile.exists()) {
          errors.add("Config file " + configFileString + " does not exist.");
        } else if (!configFile.isFile()) {
          errors.add("Config file " + configFileString + " is a directory.");
        } else if (!configFile.canRead()) {
          errors.add("Config file " + configFileString + " cannot be read.");
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new ConfigFileValidationException(errors);
    }
  }

}
