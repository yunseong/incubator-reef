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

using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Tang.Util
{
    internal sealed class SetValuedKey
    {
        public IList<object> key;

        public SetValuedKey(object[] ts, object[] us)
        {
            key = ts.ToList<object>();
            foreach (var o in us)
            {
                key.Add(o);
            }
        }

        public override int GetHashCode()
        {
            int i = 0;
            foreach (object t in key)
            {
                i += t.GetHashCode();
            }
            return i;
        }

        public override bool Equals(object o)
        {
            SetValuedKey other = (SetValuedKey)o;
            if (other.key.Count != this.key.Count) 
            { 
                return false; 
            }
            for (int i = 0; i < this.key.Count; i++)
            {
                if (this.key[i].Equals(other.key[i]))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
