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
package org.apache.pulsar.functions.worker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FunctionMetaDataManagerTest {

    private static PulsarClient mockPulsarClient() throws PulsarClientException {
        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
        when(builder.topic(anyString())).thenReturn(builder);
        when(builder.producerName(anyString())).thenReturn(builder);

        when(builder.create()).thenReturn(mock(Producer.class));

        PulsarClient client = mock(PulsarClient.class);
        when(client.newProducer()).thenReturn(builder);

        return client;
    }

    @Test
    public void testListFunctions() throws PulsarClientException {
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(new WorkerConfig(),
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));

        Map<String, Function.FunctionMetaData> functionMetaDataMap1 = new HashMap<>();
        Function.FunctionMetaData f1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1")).build();
        functionMetaDataMap1.put("func-1", f1);
        Function.FunctionMetaData f2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-2")).build();
        functionMetaDataMap1.put("func-2", f2);
        Function.FunctionMetaData f3 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-3")).build();
        Map<String, Function.FunctionMetaData> functionMetaDataInfoMap2 = new HashMap<>();
        functionMetaDataInfoMap2.put("func-3", f3);


        functionMetaDataManager.functionMetaDataMap.put("tenant-1", new HashMap<>());
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-1", functionMetaDataMap1);
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-2", functionMetaDataInfoMap2);

        Assert.assertEquals(0, functionMetaDataManager.listFunctions(
                "tenant", "namespace").size());
        Assert.assertEquals(2, functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").size());
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").contains(f1));
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").contains(f2));
        Assert.assertEquals(1, functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-2").size());
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-2").contains(f3));
    }

    @Test
    public void testUpdateIfLeaderFunction() throws PulsarClientException {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")).build();

        // update when you are not the leader
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            Assert.assertTrue(false);
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Not the leader");
        }

        // become leader
        functionMetaDataManager.acquireLeadership();
        // Now w should be able to really update
        functionMetaDataManager.updateFunctionOnLeader(m1, false);

        // outdated request
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Update request ignored because it is out of date. Please try again.");
        }
        // udpate with new version
        m1 = m1.toBuilder().setVersion(2).build();
        functionMetaDataManager.updateFunctionOnLeader(m1, false);
    }

    @Test
    public void deregisterFunction() throws PulsarClientException {
        SchedulerManager mockedScheduler = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mockedScheduler,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).build();

        // Try deleting when you are not the leader
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            Assert.assertTrue(false);
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Not the leader");
        }

        // become leader
        functionMetaDataManager.acquireLeadership();
        verify(mockedScheduler, times(0)).schedule();
        // Now try deleting
        functionMetaDataManager.updateFunctionOnLeader(m1, true);
        // make sure schedule was not called because function didn't exist.
        verify(mockedScheduler, times(0)).schedule();

        // insert function
        functionMetaDataManager.updateFunctionOnLeader(m1, false);
        verify(mockedScheduler, times(1)).schedule();

        // outdated request
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Delete request ignored because it is out of date. Please try again.");
        }
        verify(mockedScheduler, times(1)).schedule();

        // udpate with new version
        m1 = m1.toBuilder().setVersion(2).build();
        functionMetaDataManager.updateFunctionOnLeader(m1, true);
        verify(mockedScheduler, times(2)).schedule();
    }

    @Test
    public void testProcessRequest() throws PulsarClientException, IOException {
        WorkerConfig workerConfig = new WorkerConfig();
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));

        doReturn(true).when(functionMetaDataManager).processUpdate(any(Function.FunctionMetaData.class));
        doReturn(true).when(functionMetaDataManager).proccessDeregister(any(Function.FunctionMetaData.class));

        Request.ServiceRequest serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                        Request.ServiceRequest.ServiceRequestType.UPDATE).build();
        Message msg = mock(Message.class);
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        verify(functionMetaDataManager, times(1)).processUpdate
                (any(Function.FunctionMetaData.class));
        verify(functionMetaDataManager).processUpdate(serviceRequest.getFunctionMetaData());

        serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                Request.ServiceRequest.ServiceRequestType.INITIALIZE).build();
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                Request.ServiceRequest.ServiceRequestType.DELETE).build();
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        verify(functionMetaDataManager, times(1)).proccessDeregister(
                any(Function.FunctionMetaData.class));
        verify(functionMetaDataManager).proccessDeregister(serviceRequest.getFunctionMetaData());
    }

    @Test
    public void processUpdateTest() throws PulsarClientException {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                .setNamespace("namespace-1").setTenant("tenant-1")).build();

        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // outdated request
        try {
            functionMetaDataManager.processUpdate(m1);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Update request ignored because it is out of date. Please try again.");
        }
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // udpate with new version
        m1 = m1.toBuilder().setVersion(2).build();
        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(2))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }

    @Test
    public void processDeregister() throws PulsarClientException {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).build();

        Assert.assertFalse(functionMetaDataManager.proccessDeregister(m1));
        verify(functionMetaDataManager, times(0))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(0, functionMetaDataManager.functionMetaDataMap.size());

        // insert something
        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // outdated delete request
        try {
            functionMetaDataManager.proccessDeregister(m1);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Delete request ignored because it is out of date. Please try again.");
        }
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // delete now
        m1 = m1.toBuilder().setVersion(2).build();
        Assert.assertTrue(functionMetaDataManager.proccessDeregister(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(0, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }
}