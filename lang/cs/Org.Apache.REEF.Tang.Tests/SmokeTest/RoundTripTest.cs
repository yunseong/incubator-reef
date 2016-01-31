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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Tang.Tests.SmokeTest
{
    [TestClass]
    public abstract class RoundTripTest
    {
        public abstract IConfiguration RoundTrip(IConfiguration configuration);

        [TestMethod]
        public void TestRoundTrip() 
        {
            IConfiguration conf = ObjectTreeTest.GetConfiguration();
            IRootInterface before = TangFactory.GetTang().NewInjector(conf).GetInstance<IRootInterface>();
            IRootInterface after = TangFactory.GetTang().NewInjector(RoundTrip(conf)).GetInstance<IRootInterface>();
            Assert.AreEqual(before, after, "Configuration conversion to and from Avro datatypes failed.");
        }
    }
}
