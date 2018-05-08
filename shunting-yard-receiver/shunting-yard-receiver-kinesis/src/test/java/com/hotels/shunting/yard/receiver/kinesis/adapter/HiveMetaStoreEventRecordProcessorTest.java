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
package com.hotels.shunting.yard.receiver.kinesis.adapter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreEventRecordProcessorTest {

  private @Mock KinesisRecordBuffer buffer;
  private @Mock Record record;
  private @Mock InitializationInput initializationInput;
  private @Mock ShutdownInput shutdownInput;
  private @Mock ProcessRecordsInput processRecordsInput;
  private @Mock IRecordProcessorCheckpointer checkpointer;

  private HiveMetaStoreEventRecordProcessor processor;

  @Before
  public void init() {
    processor = new HiveMetaStoreEventRecordProcessor(buffer);
  }

  @Test
  public void initShard() {
    processor.initialize(initializationInput);
    verify(initializationInput).getShardId();
  }

  @Test
  public void commitsOnShutdownUponTermination() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.TERMINATE);
    when(shutdownInput.getCheckpointer()).thenReturn(checkpointer);
    processor.shutdown(shutdownInput);
    verify(checkpointer).checkpoint();
  }

  @Test
  public void doNotCommitOnShutdownUponRequest() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.REQUESTED);
    processor.shutdown(shutdownInput);
    verify(shutdownInput, never()).getCheckpointer();
  }

  @Test
  public void doNotCommitOnShutdownWhenZombie() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.ZOMBIE);
    processor.shutdown(shutdownInput);
    verify(shutdownInput, never()).getCheckpointer();
  }

  @Test
  public void readRecordIntoBuffer() throws Exception {
    when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
    when(processRecordsInput.getRecords()).thenReturn(Arrays.asList(record));
    when(buffer.put(any(Record.class))).thenReturn(true);
    processor.processRecords(processRecordsInput);
    verify(buffer).put(record);
    verify(checkpointer).checkpoint();
  }

  @Test
  public void readRecordIntoBufferWhenBufferIsFull() throws Exception {
    when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
    when(processRecordsInput.getRecords()).thenReturn(Arrays.asList(record));
    when(buffer.put(any(Record.class))).thenReturn(false, true);
    processor.processRecords(processRecordsInput);
    verify(buffer, times(2)).put(record);
    verify(checkpointer).checkpoint();
  }

  @Test
  public void checkpointerExceptionsAreSilentDuringProcessing() throws Exception {
    when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
    when(processRecordsInput.getRecords()).thenReturn(Arrays.asList(record));
    when(buffer.put(any(Record.class))).thenReturn(true);
    doThrow(RuntimeException.class).when(checkpointer).checkpoint();
    processor.processRecords(processRecordsInput);
  }

  @Test
  public void checkpointerShutdownExceptionAreSilentDuringProcessing() throws Exception {
    when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
    when(processRecordsInput.getRecords()).thenReturn(Arrays.asList(record));
    when(buffer.put(any(Record.class))).thenReturn(true);
    doThrow(ShutdownException.class).when(checkpointer).checkpoint();
    processor.processRecords(processRecordsInput);
  }

  @Test
  public void checkpointerThrottlingExceptionAreSilentDuringProcessing() throws Exception {
    when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
    when(processRecordsInput.getRecords()).thenReturn(Arrays.asList(record));
    when(buffer.put(any(Record.class))).thenReturn(true);
    doThrow(ThrottlingException.class).when(checkpointer).checkpoint();
    processor.processRecords(processRecordsInput);
  }

  @Test
  public void checkpointerInvalidStateExceptionAreSilentDuringProcessing() throws Exception {
    when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
    when(processRecordsInput.getRecords()).thenReturn(Arrays.asList(record));
    when(buffer.put(any(Record.class))).thenReturn(true);
    doThrow(ThrottlingException.class).when(checkpointer).checkpoint();
    processor.processRecords(processRecordsInput);
  }

  @Test
  public void checkpointerExceptionsAreSilentDuringShutdown() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.TERMINATE);
    when(shutdownInput.getCheckpointer()).thenReturn(checkpointer);
    doThrow(RuntimeException.class).when(checkpointer).checkpoint();
    processor.shutdown(shutdownInput);
  }

  @Test
  public void checkpointerShutdownExceptionAreSilentDuringShutdown() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.TERMINATE);
    when(shutdownInput.getCheckpointer()).thenReturn(checkpointer);
    doThrow(ShutdownException.class).when(checkpointer).checkpoint();
    processor.shutdown(shutdownInput);
  }

  @Test
  public void checkpointerThrottlingExceptionAreSilentDuringShutdown() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.TERMINATE);
    when(shutdownInput.getCheckpointer()).thenReturn(checkpointer);
    doThrow(ThrottlingException.class).when(checkpointer).checkpoint();
    processor.shutdown(shutdownInput);
  }

  @Test
  public void checkpointerInvalidStateExceptionAreSilentDuringShutdown() throws Exception {
    when(shutdownInput.getShutdownReason()).thenReturn(ShutdownReason.TERMINATE);
    when(shutdownInput.getCheckpointer()).thenReturn(checkpointer);
    doThrow(ThrottlingException.class).when(checkpointer).checkpoint();
    processor.shutdown(shutdownInput);
  }

}
