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
package com.expediagroup.shuntingyard.replicator.exec.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.shuntingyard.common.ShuntingYardException;
import com.expediagroup.shuntingyard.replicator.exec.event.MetaStoreEvent;
import com.expediagroup.shuntingyard.replicator.exec.event.aggregation.MetaStoreEventAggregator;
import com.google.common.annotations.VisibleForTesting;

public class AggregatingMetaStoreEventReader implements MetaStoreEventReader {
  private static final Logger log = LoggerFactory.getLogger(AggregatingMetaStoreEventReader.class);

  private class EventAggregationCallable implements Callable<List<MetaStoreEvent>> {
    @Override
    public List<MetaStoreEvent> call() throws Exception {
      List<MetaStoreEvent> events = new ArrayList<>();
      long maxExecTime = windowUnits.toMillis(window);
      long startTime = System.currentTimeMillis();
      while (startTime + maxExecTime > System.currentTimeMillis()) {
        Optional<MetaStoreEvent> event = delegate.read();
        if (event.isPresent()) {
          events.add(event.get());
        }
      }
      return aggregator.aggregate(events);
    }
  }

  private final Object monitor = new Object();
  private final Queue<MetaStoreEvent> buffer;
  private final MetaStoreEventReader delegate;
  private final MetaStoreEventAggregator aggregator;
  private final long window;
  private final TimeUnit windowUnits;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private Future<List<MetaStoreEvent>> lastSubmittedTask;

  public AggregatingMetaStoreEventReader(MetaStoreEventReader delegate, MetaStoreEventAggregator aggregator) {
    this(delegate, aggregator, 30, TimeUnit.SECONDS);
  }

  public AggregatingMetaStoreEventReader(
      MetaStoreEventReader delegate,
      MetaStoreEventAggregator aggregator,
      long window,
      TimeUnit windowUnits) {
    this(delegate, aggregator, window, windowUnits, new ConcurrentLinkedQueue<>());
  }

  @VisibleForTesting
  AggregatingMetaStoreEventReader(
      MetaStoreEventReader delegate,
      MetaStoreEventAggregator aggregator,
      long window,
      TimeUnit windowUnits,
      Queue<MetaStoreEvent> buffer) {
    this.delegate = delegate;
    this.aggregator = aggregator;
    this.window = window;
    this.windowUnits = windowUnits;
    this.buffer = buffer;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
    executor.shutdownNow();
  }

  @Override
  public Optional<MetaStoreEvent> read() {
    requestMoreMessagesIfNeeded();
    while (buffer.isEmpty()) {
      try {
        List<MetaStoreEvent> events = null;
        synchronized (monitor) {
          events = lastSubmittedTask.get(window, windowUnits);
          lastSubmittedTask = null;
        }
        if (events.isEmpty()) {
          return Optional.empty();
        }
        buffer.addAll(events);
      } catch (TimeoutException e) {
        log.debug("Timeout whilst buffering message. Retrying...", e);
      } catch (InterruptedException e) {
        log.warn("Thread was interrupted whilst buffering message. Retrying...", e);
      } catch (ExecutionException e) {
        // TODO at this point all previously read messages will be lost: this will be addressed in
        // https://github.com/HotelsDotCom/shunting-yard/issues/3
        lastSubmittedTask = null;
        requestMoreMessagesIfNeeded();
        Throwable cause = e.getCause();
        if (cause != null && RuntimeException.class.isAssignableFrom(cause.getClass())) {
          throw (RuntimeException) cause;
        }
        throw new ShuntingYardException("Delegate MessageReader has failed to read messages", cause);
      }
    }
    return Optional.ofNullable(buffer.poll());

  }

  private void requestMoreMessagesIfNeeded() {
    if (lastSubmittedTask == null) {
      synchronized (monitor) {
        if (lastSubmittedTask == null) {
          lastSubmittedTask = executor.submit(new EventAggregationCallable());
        }
      }
    }
  }

}
