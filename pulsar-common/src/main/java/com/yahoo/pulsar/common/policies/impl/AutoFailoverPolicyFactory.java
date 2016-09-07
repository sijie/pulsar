/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.policies.impl;

import com.yahoo.pulsar.common.policies.AutoFailoverPolicy;
import com.yahoo.pulsar.common.policies.data.AutoFailoverPolicyData;
import com.yahoo.pulsar.common.policies.data.AutoFailoverPolicyType;

public class AutoFailoverPolicyFactory {

    public static AutoFailoverPolicy create(AutoFailoverPolicyData policyData) {
        // TODO: Add more policy types when needed
        if (!AutoFailoverPolicyType.min_available.equals(policyData.policy_type)) {
            // right now, only support one type of policy: MinAvailablePolicy
            throw new IllegalArgumentException("Unrecognized auto_failover_policy: " + policyData.policy_type);
        }

        return new MinAvailablePolicy(policyData);
    }

}
