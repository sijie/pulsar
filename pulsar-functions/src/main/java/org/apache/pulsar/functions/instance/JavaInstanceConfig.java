/**
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
package org.apache.pulsar.functions.instance;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.FunctionID;
import org.apache.pulsar.functions.runtime.InstanceID;

/**
 * This is the config passed to the Java Instance. Contains all the information
 * passed to run functions
 */
@Data
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class JavaInstanceConfig {
    private String functionName;
    private FunctionID functionId;
    private InstanceID instanceId;
    private String functionVersion;
    private FunctionConfig functionConfig;
    private String nameSpace;
    private String userName;
    private int timeBudgetInMs;
    private int maxMemory;
}