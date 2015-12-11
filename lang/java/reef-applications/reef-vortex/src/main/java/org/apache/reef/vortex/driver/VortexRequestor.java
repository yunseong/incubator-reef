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
package org.apache.reef.vortex.driver;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.vortex.common.TaskletExecutionRequest;
import org.apache.reef.vortex.common.VortexRequest;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Takes the serialization work from the scheduler thread.
 */
@DriverSide
class VortexRequestor {
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Inject
  VortexRequestor() {
  }

  void send(final RunningTask reefTask, final VortexRequest vortexRequest, final TraceInfo traceInfo) {
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        final byte[] requestBytes;

        //  Possible race condition with VortexWorkerManager#terminate is addressed by the global lock in VortexMaster
        final Kryo kryo = new Kryo();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final Output output = new Output(byteArrayOutputStream);
        kryo.register(TaskletExecutionRequest.class);
        kryo.register(VortexRequest.class);
        kryo.writeObject(output, vortexRequest);
        output.close();
        requestBytes = byteArrayOutputStream.toByteArray();

        reefTask.send(requestBytes);
      }
    });
  }

  void send(final RunningTask reefTask, final byte[] serializedRequest, final TraceInfo traceInfo) {
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        reefTask.send(serializedRequest);
      }
    });
  }
}
