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
