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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using ProtoBuf;

namespace Org.Apache.REEF.Tang.Protobuf
{
    public sealed class ProtocolBufferClassHierarchy : IClassHierarchy
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ProtocolBufferClassHierarchy));

        private readonly IPackageNode rootNode;
        private readonly IDictionary<string, INode> lookupTable = new Dictionary<string, INode>();
        private readonly IDictionary<string, IDictionary<string, string>> _aliasLookupTable = new Dictionary<string, IDictionary<string, string>>();

        public static void Serialize(string fileName, IClassHierarchy classHierarchy)
        {
            Node node = Serialize(classHierarchy);

            using (var file = File.Create(fileName))
            {
                Serializer.Serialize<Node>(file, node);
            }
        }

        public static Node Serialize(IClassHierarchy classHierarchy)
        {
            return SerializeNode(classHierarchy.GetNamespace());
        }

        private static Node SerializeNode(INode n)
        {
            IList<Node> children = new List<Node>();

            foreach (INode child in n.GetChildren())
            {
                children.Add(SerializeNode(child));
            }

            if (n is IClassNode)
            {
                IClassNode cn = (IClassNode)n;
                IList<IConstructorDef> injectable = cn.GetInjectableConstructors();
                IList<IConstructorDef> all = cn.GetAllConstructors();
                IList<IConstructorDef> others = new List<IConstructorDef>(all);

                foreach (var c in injectable)
                {
                    others.Remove(c);
                }

                IList<ConstructorDef> injectableConstructors = new List<ConstructorDef>();
                foreach (IConstructorDef inj in injectable)
                {
                    injectableConstructors.Add(SerializeConstructorDef(inj));
                }

                IList<ConstructorDef> otherConstructors = new List<ConstructorDef>();
                foreach (IConstructorDef other in others)
                {
                    otherConstructors.Add(SerializeConstructorDef(other));
                }

                List<string> implFullNames = new List<string>();
                foreach (IClassNode impl in cn.GetKnownImplementations())
                {
                    implFullNames.Add(impl.GetFullName());  // we use class fully qualifed name 
                }

                return NewClassNode(cn.GetName(), cn.GetFullName(),
                    cn.IsInjectionCandidate(), cn.IsExternalConstructor(), cn.IsUnit(),
                    injectableConstructors, otherConstructors, implFullNames, children);
            }
            if (n is INamedParameterNode)
            {
                INamedParameterNode np = (INamedParameterNode)n;
                return NewNamedParameterNode(np.GetName(), np.GetFullName(),
                    np.GetSimpleArgName(), np.GetFullArgName(), np.IsSet(), np.IsList(), np.GetDocumentation(),
                    np.GetShortName(), np.GetDefaultInstanceAsStrings(), children, np.GetAlias(), np.GetAliasLanguage());
            }
            if (n is IPackageNode)
            {
                return NewPackageNode(n.GetName(), n.GetFullName(), children);
            }
            Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Encountered unknown type of Node: " + n), LOGGER);
            return null;
        }

        private static ConstructorDef SerializeConstructorDef(IConstructorDef def)
        {
            IList<ConstructorArg> args = new List<ConstructorArg>();
            foreach (IConstructorArg arg in def.GetArgs())
            {
                args.Add(NewConstructorArg(arg.Gettype(), arg.GetNamedParameterName(), arg.IsInjectionFuture()));
            }
            return newConstructorDef(def.GetClassName(), args);
        }

        private static ConstructorArg NewConstructorArg(
            string fullArgClassName, string namedParameterName, bool isFuture)
        {
            ConstructorArg constArg = new ConstructorArg();
            constArg.full_arg_class_name = fullArgClassName;
            constArg.named_parameter_name = namedParameterName;
            constArg.is_injection_future = isFuture;
            return constArg;
        }

        private static ConstructorDef newConstructorDef(
             string fullClassName, IList<ConstructorArg> args)
        {
            ConstructorDef constDef = new ConstructorDef();
            constDef.full_class_name = fullClassName;
            foreach (ConstructorArg arg in args)
            {
                constDef.args.Add(arg);
            }

            return constDef;
        }

        private static Node NewClassNode(string name,
            string fullName, bool isInjectionCandidate,
            bool isExternalConstructor, bool isUnit,
            IList<ConstructorDef> injectableConstructors,
            IList<ConstructorDef> otherConstructors,
            IList<string> implFullNames, IList<Node> children)
        {
            ClassNode classNode = new ClassNode();
            classNode.is_injection_candidate = isInjectionCandidate;
            foreach (var ic in injectableConstructors)
            {
                classNode.InjectableConstructors.Add(ic);
            }

            foreach (var oc in otherConstructors)
            {
                classNode.OtherConstructors.Add(oc);
            }
            foreach (var implFullName in implFullNames)
            {
                classNode.impl_full_names.Add(implFullName);
            }

            Node n = new Node();
            n.name = name;
            n.full_name = fullName;
            n.class_node = classNode;

            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private static Node NewNamedParameterNode(string name,
            string fullName, string simpleArgClassName, string fullArgClassName,
            bool isSet, bool isList, string documentation, // can be null
            string shortName, // can be null
            string[] instanceDefault, // can be null
            IList<Node> children,
            string alias, Language aliasLanguage)
        {
            NamedParameterNode namedParameterNode = new NamedParameterNode();
            namedParameterNode.simple_arg_class_name = simpleArgClassName;
            namedParameterNode.full_arg_class_name = fullArgClassName;
            namedParameterNode.is_set = isSet;
            namedParameterNode.is_list = isList;

            if (documentation != null)
            {
                namedParameterNode.documentation = documentation;
            }

            if (shortName != null)
            {
                namedParameterNode.short_name = shortName;
            }

            if (alias != null)
            {
                namedParameterNode.alias_name = alias;
            }

            namedParameterNode.alias_language = aliasLanguage.ToString();

            foreach (var id in instanceDefault)
            {
                namedParameterNode.instance_default.Add(id);
            }

            Node n = new Node();
            n.name = name;
            n.full_name = fullName;
            n.named_parameter_node = namedParameterNode;

            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private static Node NewPackageNode(string name,
            string fullName, IList<Node> children)
        {
            PackageNode packageNode = new PackageNode();
            Node n = new Node();
            n.name = name;
            n.full_name = fullName;
            n.package_node = packageNode;

            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        public static IClassHierarchy DeSerialize(string fileName)
        {
            Node root;

            using (var file = File.OpenRead(fileName))
            {
                root = Serializer.Deserialize<Node>(file);
            }

            return new ProtocolBufferClassHierarchy(root);
        }

        // create a ProtocolBufferClassHierarchy with empty nodes and lookup table. It can be used to merge other class hierarchy to it
        public ProtocolBufferClassHierarchy()
        {
            this.rootNode = new PackageNodeImpl();
        }

        public ProtocolBufferClassHierarchy(Node root)
        {
            this.rootNode = new PackageNodeImpl();
            if (root.package_node == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Expected a package node.  Got: " + root), LOGGER); 
            }

            // Register all the classes.
            foreach (Node child in root.children)
            {
                ParseSubHierarchy(rootNode, child);
            }
            
            BuildHashTable(rootNode);

            foreach (Node child in root.children)
            {
                WireUpInheritanceRelationships(child);
            }
        }

        public void BuildHashTable(INode n)
        {
            foreach (INode child in n.GetChildren())
            {
                lookupTable.Add(child.GetFullName(), child);
                if (child is INamedParameterNode)
                {
                    AddAlias((INamedParameterNode)child);
                }
                BuildHashTable(child);
            }
        }

        private void AddAlias(INamedParameterNode np)
        {
            if (!string.IsNullOrEmpty(np.GetAlias()))
            {
                IDictionary<string, string> mapping = null;
                _aliasLookupTable.TryGetValue(np.GetAliasLanguage().ToString(), out mapping);
                if (mapping == null)
                {
                    mapping = new Dictionary<string, string>();
                    _aliasLookupTable.Add(np.GetAliasLanguage().ToString(), mapping);
                }
                try
                {
                    mapping.Add(np.GetAlias(), np.GetFullName());
                }
                catch (Exception)
                {
                    var e = new ApplicationException(string.Format(CultureInfo.CurrentCulture, "Duplicated alias {0} on named parameter {1}.", np.GetAlias(), np.GetFullName()));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        private static void ParseSubHierarchy(INode parent, Node n)
        {
            INode parsed = null;
            if (n.package_node != null)
            {
                parsed = new PackageNodeImpl(parent, n.name, n.full_name);
            }
            else if (n.named_parameter_node != null)
            {
                NamedParameterNode np = n.named_parameter_node;

                if (!string.IsNullOrWhiteSpace(np.alias_name) && !string.IsNullOrWhiteSpace(np.alias_language))
                {
                    Language language;
                    try
                    {                        
                        Enum.TryParse(np.alias_language, true, out language);
                    }
                    catch (Exception)
                    {
                        string msg = string.Format(CultureInfo.CurrentCulture, "Language {0} passed in is not supported", np.alias_language);
                        throw new ArgumentException(msg);
                    }

                    parsed = new NamedParameterNodeImpl(parent, n.name,
                        n.full_name, np.full_arg_class_name, np.simple_arg_class_name,
                        np.is_set, np.is_list, np.documentation, np.short_name,
                        np.instance_default.ToArray(), np.alias_name, language);
                }
                else
                {
                    parsed = new NamedParameterNodeImpl(parent, n.name,
                       n.full_name, np.full_arg_class_name, np.simple_arg_class_name,
                       np.is_set, np.is_list, np.documentation, np.short_name,
                       np.instance_default.ToArray());
                }
            }
            else if (n.class_node != null)
            {
                ClassNode cn = n.class_node;
                IList<IConstructorDef> injectableConstructors = new List<IConstructorDef>();
                IList<IConstructorDef> allConstructors = new List<IConstructorDef>();

                foreach (ConstructorDef injectable in cn.InjectableConstructors)
                {
                    IConstructorDef def = ParseConstructorDef(injectable, true);
                    injectableConstructors.Add(def);
                    allConstructors.Add(def);
                }
                foreach (ConstructorDef other in cn.OtherConstructors)
                {
                    IConstructorDef def = ParseConstructorDef(other, false);
                    allConstructors.Add(def);
                }

                IConstructorDef[] dummy = new ConstructorDefImpl[0];
                parsed = new ClassNodeImpl(parent, n.name, n.full_name,
                cn.is_unit, cn.is_injection_candidate,
                cn.is_external_constructor, injectableConstructors,
                allConstructors, cn.default_implementation);
            }
            else
            {
                Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Bad protocol buffer: got abstract node" + n), LOGGER); 
            }

            foreach (Node child in n.children)
            {
                ParseSubHierarchy(parsed, child);
            }
        }

        private static IConstructorDef ParseConstructorDef(ConstructorDef def, bool isInjectable)
        {
            IList<IConstructorArg> args = new List<IConstructorArg>();
            foreach (ConstructorArg arg in def.args)
            {
                args.Add(new ConstructorArgImpl(arg.full_arg_class_name, arg.named_parameter_name, arg.is_injection_future));
            }
            return new ConstructorDefImpl(def.full_class_name, args.ToArray(), isInjectable);
        }

        private void WireUpInheritanceRelationships(Node n)
        {
            if (n.class_node != null)
            {
                ClassNode cn = n.class_node;
                IClassNode iface = null;
                try
                {
                    iface = (IClassNode)GetNode(n.full_name);
                }
                catch (NameResolutionException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    var ex = new IllegalStateException("When reading protocol buffer node "
                        + n.full_name + " does not exist.  Full record is " + n, e);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER); 
                }
                foreach (string impl in cn.impl_full_names)
                {
                    try
                    {
                        iface.PutImpl((IClassNode)GetNode(impl));
                    }
                    catch (NameResolutionException e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        var ex = new IllegalStateException("When reading protocol buffer node "
                            + n + " refers to non-existent implementation:" + impl);
                        Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER); 
                    }
                    catch (InvalidCastException e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        try
                        {
                            var ex = new IllegalStateException(
                                "When reading protocol buffer node " + n
                                + " found implementation" + GetNode(impl)
                                + " which is not a ClassNode!");
                            Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                        }
                        catch (NameResolutionException ne)
                        {
                            Utilities.Diagnostics.Exceptions.Caught(ne, Level.Error, LOGGER);
                            var ex = new IllegalStateException(
                                "Got 'cant happen' exception when producing error message for " + e);
                            Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                        }
                    }
                }
            }
        }

        public INode GetNode(string fullName)
        {
            INode ret;
            lookupTable.TryGetValue(fullName, out ret);
            if (ret == null)
            {
                var ex = new NameResolutionException(fullName, "Cannot resolve the name from the class hierarchy during deserialization: " + fullName);
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            return ret;
        }

        /// <summary>
        /// This method get INode from deSerialized class hierarchy by fullName. 
        /// If the name is not found, it will found alias for aliasLanguage. If alias is found,
        /// it will use the alias to do look up again. 
        /// </summary>
        /// <param name="fullName"></param>
        /// <param name="aliasLanguage"></param>
        /// <returns></returns>
        public INode GetNode(string fullName, string aliasLanguage)
        {
            INode ret = null;
            lookupTable.TryGetValue(fullName, out ret);
            if (ret == null)
            {
                IDictionary<string, string> mapping = null;
                string assemblyName = null;
                _aliasLookupTable.TryGetValue(aliasLanguage, out mapping);
                if (mapping != null)
                {
                    mapping.TryGetValue(fullName, out assemblyName);
                    if (assemblyName != null)
                    {
                        lookupTable.TryGetValue(assemblyName, out ret);
                    }
                }
                if (mapping == null || assemblyName == null || ret == null)
                {
                    var ex = new NameResolutionException(fullName, "Cannot resolve the name from the class hierarchy during de-serialization: " + fullName);
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            return ret;
        }

        public INode GetNamespace()
        {
            return rootNode;
        }

        public bool IsImplementation(IClassNode inter, IClassNode impl)
        {
            return impl.IsImplementationOf(inter);
        }

        public IClassHierarchy Merge(IClassHierarchy ch)
        {
            if (this == ch)
            {
                return this;
            }

            if (!(ch is ProtocolBufferClassHierarchy))
            {         
                Utilities.Diagnostics.Exceptions.Throw(new NotSupportedException(
                                                            "Cannot merge ExternalClassHierarchies yet!"), LOGGER);
            }

            ProtocolBufferClassHierarchy pch = (ProtocolBufferClassHierarchy)ch;
            foreach (var pair in pch.lookupTable)
            {
                if (!this.lookupTable.ContainsKey(pair.Key))
                {
                    this.lookupTable.Add(pair);
                }
            }

            foreach (INode n in ch.GetNamespace().GetChildren())
            {
                if (!rootNode.Contains(n.GetFullName()))
                {
                    if (n is INamedParameterNode)
                    {
                        INamedParameterNode np = (INamedParameterNode)n;
                        new NamedParameterNodeImpl(this.rootNode, np.GetName(),
                                                   np.GetFullName(), np.GetFullArgName(), np.GetSimpleArgName(),
                                                   np.IsSet(), np.IsList(), np.GetDocumentation(), np.GetShortName(),
                                                   np.GetDefaultInstanceAsStrings().ToArray());
                    }
                    else if (n is IClassNode)
                    {
                        IClassNode cn = (IClassNode)n;
                        new ClassNodeImpl(rootNode, cn.GetName(), cn.GetFullName(),
                                          cn.IsUnit(), cn.IsInjectionCandidate(),
                                          cn.IsExternalConstructor(), cn.GetInjectableConstructors(),
                                          cn.GetAllConstructors(), cn.GetDefaultImplementation());
                    }
                }
            }

            return this;
        }
    }
}