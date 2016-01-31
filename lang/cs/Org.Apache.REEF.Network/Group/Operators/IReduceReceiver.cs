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

namespace Org.Apache.REEF.Network.Group.Operators
{
    /// <summary>
    /// Group Communication operator used to receive and reduce messages.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public interface IReduceReceiver<T> : IGroupCommOperator<T>
    {
        /// <summary>
        /// Returns the class used to reduce incoming messages sent by ReduceSenders.
        /// </summary>
        IReduceFunction<T> ReduceFunction { get; } 

        /// <summary>
        /// Receives messages sent by all ReduceSenders and aggregates them
        /// using the specified IReduceFunction.
        /// </summary>
        /// <returns>The single aggregated data</returns>
        T Reduce();
    }
}
