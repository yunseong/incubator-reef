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
using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Evaluator;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal class CompletedEvaluator : ICompletedEvaluator
    {
        private string _instanceId;

        public CompletedEvaluator(ICompletedEvaluatorClr2Java clr2Java)
        {
            _instanceId = Guid.NewGuid().ToString("N");
            CompletedEvaluatorClr2Java = clr2Java;
        }

        [DataMember]
        public string InstanceId
        {
            get { return _instanceId; }
            set { _instanceId = value; }
        }

        [DataMember]
        public string Id
        {
            get
            {
                return CompletedEvaluatorClr2Java.GetId();
            }
        }

        [DataMember]
        public ICompletedEvaluatorClr2Java CompletedEvaluatorClr2Java { get; set; }
    }
}
