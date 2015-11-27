﻿/**
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

using System.Diagnostics;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.PipelineBroadcastReduceDriverAndTasks
{
    public class PipelinedSlaveTask : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedSlaveTask));

        private readonly int _numIterations;
        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastReceiver<int[]> _broadcastReceiver;
        private readonly IReduceSender<int[]> _triangleNumberSender;
        private readonly int _arraySize;

        [Inject]
        public PipelinedSlaveTask(
            [Parameter(typeof(GroupTestConfig.NumIterations))] int numIters,
            [Parameter(typeof(GroupTestConfig.ArraySize))] int arraySize,
            IGroupCommClient groupCommClient)
        {
            Logger.Log(Level.Info, "Hello from slave task");

            _arraySize = arraySize;
            _numIterations = numIters;
            _groupCommClient = groupCommClient;
            _commGroup = _groupCommClient.GetCommunicationGroup(GroupTestConstants.GroupName);
            _broadcastReceiver = _commGroup.GetBroadcastReceiver<int[]>(GroupTestConstants.BroadcastOperatorName);
            _triangleNumberSender = _commGroup.GetReduceSender<int[]>(GroupTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            int[] resArr = new int[_arraySize];

            for (int j = 0; j < resArr.Length; j++)
            {
                resArr[j] = j;
            }

            Stopwatch broadcastTime = new Stopwatch();
            Stopwatch reduceTime = new Stopwatch();

            for (int i = 0; i < _numIterations; i++)
            {
                if (i == 1)
                {
                    broadcastTime.Reset();
                    reduceTime.Reset();
                }

                broadcastTime.Start();
                // Receive n from Master Task
                int[] intVec = _broadcastReceiver.Receive();
                broadcastTime.Stop();

                Logger.Log(Level.Info, "Calculating TriangleNumber({0}) on slave task...", intVec[0]);

                // Calculate the nth Triangle number and send it back to driver
                int triangleNum = TriangleNumber(intVec[0]);

                Logger.Log(Level.Info, "Sending sum: {0} on iteration {1}.", triangleNum, i);

                resArr[0] = triangleNum;

                reduceTime.Start();
                _triangleNumberSender.Send(resArr);
                reduceTime.Stop();

                if (i >= 1)
                {
                    Logger.Log(Level.Info,
                        string.Format("Average time (milliseconds) taken for broadcast: {0} and reduce: {1}",
                            broadcastTime.ElapsedMilliseconds / ((double)i),
                            reduceTime.ElapsedMilliseconds / ((double)i)));
                }
            }

            return null;
        }

        public void Dispose()
        {
            _groupCommClient.Dispose();
        }

        private int TriangleNumber(int n)
        {
            return Enumerable.Range(1, n).Sum();
        }
    }
}
