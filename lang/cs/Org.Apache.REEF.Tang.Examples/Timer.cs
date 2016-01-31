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

using System;
using System.Threading;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tang.Examples
{
    public class Timer
    {
        [NamedParameter("Number of seconds to sleep", "sec", "10")]
        public class Seconds : Name<int> 
        { 
        }
        private readonly int seconds;

        [Inject]
        public Timer([Parameter(typeof(Seconds))] int seconds)
        {
            if (seconds < 0)
            {
                throw new ArgumentException("Cannot sleep for negative time!");
            }
            this.seconds = seconds;
        }

        public void sleep()  
        {
            Thread.Sleep(seconds * 1000);
        }
    }
}
