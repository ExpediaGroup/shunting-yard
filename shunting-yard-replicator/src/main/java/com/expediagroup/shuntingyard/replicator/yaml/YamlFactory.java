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
package com.expediagroup.shuntingyard.replicator.yaml;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import com.expediagroup.shuntingyard.replicator.exec.external.CircusTrainConfig;

public final class YamlFactory {

  private YamlFactory() {}

  public static Yaml newYaml() {
    PropertyUtils propertyUtils = new AdvancedPropertyUtils();
    propertyUtils.setSkipMissingProperties(true);
    propertyUtils.setAllowReadOnlyProperties(false);

    Constructor constructor = new Constructor();

    Representer representer = new AdvancedRepresenter();
    representer.setPropertyUtils(propertyUtils);
    representer.addClassTag(CircusTrainConfig.class, Tag.MAP);

    DumperOptions dumperOptions = new DumperOptions();
    dumperOptions.setIndent(2);
    dumperOptions.setDefaultFlowStyle(FlowStyle.BLOCK);
    dumperOptions.setAllowReadOnlyProperties(true);

    return new Yaml(constructor, representer, dumperOptions);
  }

}
