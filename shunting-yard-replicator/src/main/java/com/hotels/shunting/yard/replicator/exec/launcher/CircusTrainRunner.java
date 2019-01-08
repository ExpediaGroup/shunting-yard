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
package com.hotels.shunting.yard.replicator.exec.launcher;

import static org.apache.commons.exec.environment.EnvironmentUtils.getProcEnvironment;

import static com.hotels.shunting.yard.replicator.exec.Constants.CIRCUS_TRAIN_HOME_ENV_VAR;
import static com.hotels.shunting.yard.replicator.exec.Constants.CIRCUS_TRAIN_HOME_SCRIPT;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.output.TeeOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.shunting.yard.replicator.exec.receiver.Context;

public class CircusTrainRunner {
  private static final Logger log = LoggerFactory.getLogger(CircusTrainRunner.class);

  public void run(Context context) {
    try (OutputStream out = outStream(context); OutputStream err = errStream(context)) {
      CommandLine cli = CommandLine
          .parse(String.format("%s/%s", getProcEnvironment().get(CIRCUS_TRAIN_HOME_ENV_VAR), CIRCUS_TRAIN_HOME_SCRIPT));
      cli.addArgument("--config=${CONFIG_LOCATION}");
      cli.setSubstitutionMap(ImmutableMap.of("CONFIG_LOCATION", context.getConfigLocation()));

      Executor executor = new DefaultExecutor();
      executor.setWorkingDirectory(new File(context.getWorkspace()));
      executor.setStreamHandler(new PumpStreamHandler(out, err));

      log.debug("Executing {} with environment {}", cli, getProcEnvironment());
      int returnValue = executor.execute(cli, getProcEnvironment());
      log.debug("Command exited with value {} ", returnValue);
      if (returnValue != 0) {
        throw new CircusTrainException("Circus Train exited with error value " + returnValue);
      }
    } catch (Throwable e) {
      log.error("Unable to execute Circus Train", e);
    }
  }

  private OutputStream errStream(Context context) throws IOException {
    OutputStream log = new FileOutputStream(new File(context.getWorkspace(), "stderr.log"));
    return new TeeOutputStream(log, logStream());
  }

  private OutputStream outStream(Context context) throws IOException {
    OutputStream log = new FileOutputStream(new File(context.getWorkspace(), "stdout.log"));
    return new TeeOutputStream(log, logStream());
  }

  private static LogOutputStream logStream() {
    return new LogOutputStream() {
      @Override
      protected void processLine(String line, int level) {
        // TODO
        System.out.println(line);
      }
    };
  }

}
