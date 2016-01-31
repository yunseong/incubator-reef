﻿// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Common
{
    /// <summary>
    /// Base interface for classes that support the simultaneous submission of both Context and Task configurations.
    /// </summary>
    public interface IContextAndTaskSubmittable
    {
        /// <summary>
        /// Submit a Context and an Task.
        /// The semantics of this call are the same as first submitting the context and then, on the fired ActiveContext event
        /// to submit the Task. The performance of this will be better, though as it potentially saves some roundtrips on
        /// the network.
        /// REEF will not fire an ActiveContext as a result of this. Instead, it will fire a TaskRunning event.
        /// </summary>
        /// <param name="contextConfiguration"> the Configuration of the EvaluatorContext. See ContextConfiguration for details.</param>
        /// <param name="taskConfiguration">the Configuration of the Task. See TaskConfiguration for details.</param>
        void SubmitContextAndTask(IConfiguration contextConfiguration, IConfiguration taskConfiguration);

        /// <summary>
        /// Submit a Context with Services and an Task.
        /// The semantics of this call are the same as first submitting the context and services and then, on the fired
        /// ActiveContext event to submit the Task. The performance of this will be better, though as it potentially saves
        /// some roundtrips on the network.
        /// REEF will not fire an ActiveContext as a result of this. Instead, it will fire a TaskRunning event.
        /// </summary>
        /// <param name="contextConfiguration"></param>
        /// <param name="serviceConfiguration"></param>
        /// <param name="taskConfiguration"></param>
        void SubmitContextAndServiceAndTask(
            IConfiguration contextConfiguration, 
            IConfiguration serviceConfiguration, 
            IConfiguration taskConfiguration);
    }
}
