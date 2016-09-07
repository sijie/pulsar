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
package com.yahoo.pulsar.client.impl;

import com.yahoo.pulsar.client.api.Message;

public class ConsumerStatsDisabled extends ConsumerStats {
    private static final long serialVersionUID = 1L;

    @Override
    void updateNumMsgsReceived(Message message) {
        // Do nothing
    }

    @Override
    void incrementNumReceiveFailed() {
        // Do nothing
    }

    @Override
    void incrementNumAcksSent() {
        // Do nothing
    }

    @Override
    void incrementNumAcksSent(long numAcks) {
        // Do nothing
    }

    @Override
    void incrementNumAcksFailed() {
        // Do nothing
    }

    @Override
    void resetNumAckTracker() {
        // Do nothing
    }

    @Override
    void incrementNumAcksTracker(final int numMessages) {
        // Do nothing
    }

    @Override
    long getNumAcksTrackerSumThenReset() {
        // Do nothing
        return -1;
    }

}
