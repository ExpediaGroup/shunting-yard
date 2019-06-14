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
package com.expediagroup.shuntingyard.replicator.exec.external;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.yaml.snakeyaml.Yaml;

import com.expediagroup.shuntingyard.replicator.yaml.YamlFactory;
import com.google.common.base.Charsets;

public class Marshaller {

  private final FileSystemManager fsManager;
  private final Yaml yaml;

  public Marshaller() {
    try {
      fsManager = VFS.getManager();
    } catch (FileSystemException e) {
      throw new RuntimeException("Unable to initialize Virtual File System", e);
    }
    yaml = YamlFactory.newYaml();
  }

  public void marshall(String configLocation, CircusTrainConfig config) {
    try (FileObject target = fsManager.resolveFile(configLocation);
        Writer writer = new OutputStreamWriter(target.getContent().getOutputStream(), Charsets.UTF_8)) {
      yaml.dump(config, writer);
    } catch (IOException e) {
      throw new RuntimeException("Unable to write Circus Train config to '" + configLocation + "'", e);
    }
  }

}
