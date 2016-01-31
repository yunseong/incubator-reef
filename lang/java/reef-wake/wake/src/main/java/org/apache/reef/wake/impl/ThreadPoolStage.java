/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.AbstractEStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.StageConfiguration.*;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.exception.WakeRuntimeException;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage that executes an event handler with a thread pool.
 *
 * @param <T> type
 */
public final class ThreadPoolStage<T> extends AbstractEStage<T> {
  private static final Logger LOG = Logger.getLogger(ThreadPoolStage.class.getName());

  private final EventHandler<T> handler;
  private final ExecutorService executor;
  private final int numThreads;
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;
  private final EventHandler<Throwable> errorHandler;

  /**
   * Constructs a thread-pool stage.
   *
   * @param handler    the event handler to execute
   * @param numThreads the number of threads to use
   * @throws WakeRuntimeException
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(NumberOfThreads.class) final int numThreads) {
    this(handler.getClass().getName(), handler, numThreads, null);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name         the stage name
   * @param handler      the event handler to execute
   * @param numThreads   the number of threads to use
   * @param errorHandler the error handler
   * @throws WakeRuntimeException
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(NumberOfThreads.class) final int numThreads,
                         @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    super(name);
    this.handler = handler;
    this.errorHandler = errorHandler;
    if (numThreads <= 0) {
      throw new WakeRuntimeException(name + " numThreads " + numThreads + " is less than or equal to 0");
    }
    this.numThreads = numThreads;
    this.executor = Executors.newFixedThreadPool(numThreads, new DefaultThreadFactory(name));
    StageManager.instance().register(this);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name       the stage name
   * @param handler    the event handler to execute
   * @param numThreads the number of threads to use
   * @throws WakeRuntimeException
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(NumberOfThreads.class) final int numThreads) {
    this(name, handler, numThreads, null);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param handler  the event handler to execute
   * @param executor the external executor service provided
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor) {
    this(handler.getClass().getName(), handler, executor);
  }


  /**
   * Constructs a thread-pool stage.
   *
   * @param handler      the event handler to execute
   * @param executor     the external executor service provided
   * @param errorHandler the error handler
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor,
                         @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    this(handler.getClass().getName(), handler, executor, errorHandler);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name     the stage name
   * @param handler  the event handler to execute
   * @param executor the external executor service provided
   *                 for consistent tracking, it is recommended to create executor with {@link DefaultThreadFactory}
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor) {
    this(name, handler, executor, null);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name         the stage name
   * @param handler      the event handler to execute
   * @param executor     the external executor service provided
   *                     for consistent tracking, it is recommended to create executor with {@link DefaultThreadFactory}
   * @param errorHandler the error handler
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor,
                         @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    super(name);
    this.handler = handler;
    this.errorHandler = errorHandler;
    this.numThreads = 0;
    this.executor = executor;
    StageManager.instance().register(this);
  }

  /**
   * Handles the event using a thread in the thread pool.
   *
   * @param value the event
   */
  @Override
  @SuppressWarnings("checkstyle:illegalcatch")
  public void onNext(final T value) {
    beforeOnNext();
    executor.submit(new Runnable() {

      @Override
      public void run() {
        try {
          handler.onNext(value);
          afterOnNext();
        } catch (final Throwable t) {
          if (errorHandler != null) {
            errorHandler.onNext(t);
          } else {
            LOG.log(Level.SEVERE, name + " Exception from event handler", t);
            throw t;
          }
        }
      }

    });
  }

  /**
   * Closes resources.
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true) && numThreads > 0) {
      executor.shutdown();
      if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }

  /**
   * Gets the queue length of this stage.
   *
   * @return the queue length
   */
  public int getQueueLength() {
    return ((ThreadPoolExecutor) executor).getQueue().size();
  }

}
