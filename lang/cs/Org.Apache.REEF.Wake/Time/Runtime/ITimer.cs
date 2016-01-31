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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Wake.Time.Runtime
{
    [DefaultImplementation(typeof(RealTimer))]
    public interface ITimer
    {
        /// <summary>
        /// Gets the current time
        /// </summary>
        long CurrentTime { get; }

        /// <summary>
        /// Gets the difference between the given time and the current time
        /// </summary>
        /// <param name="time">The time to compare against the current time</param>
        long GetDuration(long time);

        /// <summary>
        /// Checks if the given time has already passed.
        /// </summary>
        /// <param name="time">The time to check if it has passed or not</param>
        bool IsReady(long time);
    }
}
