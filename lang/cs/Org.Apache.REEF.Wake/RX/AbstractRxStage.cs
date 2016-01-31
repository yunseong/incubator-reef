// Licensed to the Apache Software Foundation (ASF) under one
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

namespace Org.Apache.REEF.Wake.RX
{
    /// <summary>
    /// An Rx stage that implements metering
    /// </summary>
    public abstract class AbstractRxStage<T> : IRxStage<T>
    {
        // protected internal readonly Meter meter;

        /// <summary>Constructs an abstract rxstage</summary>
        /// <param name="meterName">the name of the meter</param>
        public AbstractRxStage(string meterName)
        {
            // meter = new Meter(meterName);
        }

        /// <summary>Updates the meter</summary>
        /// <param name="value">the event</param>
        public virtual void OnNext(T value)
        {
            // meter.Mark(1);
        }

        public abstract void OnCompleted();

        public abstract void OnError(Exception error);

        public virtual void Dispose()
        {
            // no op
        }
    }
}
