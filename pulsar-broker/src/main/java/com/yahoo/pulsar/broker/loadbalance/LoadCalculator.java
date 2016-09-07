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
package com.yahoo.pulsar.broker.loadbalance;

/**
 * This interface class defines how we calculator the resource requirement per service unit based on the
 * <code>ServiceRequest</code>
 * 
 * 
 */
public interface LoadCalculator {
    public ResourceDescription getResourceDescription(ServiceRequest srvRequest);

    public void recaliberateResourceUsagePerServiceUnit(LoadReport loadReport);
}
