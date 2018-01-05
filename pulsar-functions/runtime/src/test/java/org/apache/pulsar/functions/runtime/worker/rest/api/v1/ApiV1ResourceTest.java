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
package org.apache.pulsar.functions.runtime.worker.rest.api.v1;

import static org.apache.pulsar.functions.runtime.worker.rest.RestUtils.createMessage;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.gson.Gson;
import dlshade.com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.fs.FunctionConfig.ProcessingGuarantees;
import org.apache.pulsar.functions.runtime.serde.Utf8StringSerDe;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.worker.FunctionMetaData;
import org.apache.pulsar.functions.runtime.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.runtime.worker.PackageLocationMetaData;
import org.apache.pulsar.functions.runtime.worker.Utils;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.apache.pulsar.functions.runtime.worker.request.RequestResult;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ApiV1Resource}.
 *
 * TODO: there is some issues related to powermock and log4j2: https://github.com/powermock/powermock/issues/861
 */
@PrepareForTest(Utils.class)
public class ApiV1ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final class TestFunction implements RequestHandler<String, String> {

        public String handleRequest(String input, Context context) throws Exception {
            return input;
        }
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String sinkTopic = "test-sink-topic";
    private static final String sourceTopic = "test-source-topic";
    private static final String inputSerdeClassName = Utf8StringSerDe.class.getName();
    private static final String outputSerdeClassName = Utf8StringSerDe.class.getName();
    private static final String className = TestFunction.class.getName();
    private static final LimitsConfig limitsConfig = new LimitsConfig()
        .setMaxTimeMs(1234)
        .setMaxCpuCores(2345)
        .setMaxMemoryMb(3456)
        .setMaxBufferedTuples(5678);

    private FunctionMetaDataManager mockedManager;
    private Namespace mockedNamespace;
    private ApiV1Resource resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;

    @BeforeMethod
    public void setup() {
        this.resource = spy(new ApiV1Resource());
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        this.mockedNamespace = mock(Namespace.class);
        doReturn(mockedManager).when(resource).getWorkerFunctionStateManager();
        doReturn(mockedNamespace).when(resource).getDlogNamespace();

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setDefaultLimits(limitsConfig)
            .setDownloadDirectory("/tmp/pulsar/functions")
            .setFunctionMetadataTopic("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/")
            .setZookeeperServers("localhost:2181");
        doReturn(workerConfig).when(resource).getWorkerConfig();
    }

    //
    // Register Functions
    //

    public void testRegisterFunctionMissingTenant() {
        testRegisterFunctionMissingArguments(
            null,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Tenant");
    }


    public void testRegisterFunctionMissingNamespace() {
        testRegisterFunctionMissingArguments(
            tenant,
            null,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Namespace");
    }


    public void testRegisterFunctionMissingFunctionName() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Function Name");
    }


    public void testRegisterFunctionMissingPackage() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Function Package");
    }


    public void testRegisterFunctionMissingPackageDetails() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            null,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Function Package");
    }


    public void testRegisterFunctionMissingSourceTopic() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            null,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Source Topic");
    }


    public void testRegisterFunctionMissingInputSerde() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            null,
            outputSerdeClassName,
            className,
            "InputSerdeClassName");
    }


    public void testRegisterFunctionMissingOutputSerde() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            null,
            className,
            "OutputSerdeClassName");
    }


    public void testRegisterFunctionMissingClassName() {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            null,
            "ClassName");
    }

    private void testRegisterFunctionMissingArguments(
        String tenant,
        String namespace,
        String function,
        InputStream inputStream,
        FormDataContentDisposition details,
        String sinkTopic,
        String sourceTopic,
        String inputSerdeClassName,
        String outputSerdeClassName,
        String className,
        String missingFieldName
    ) {
        Response response = resource.registerFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response registerDefaultFunction() {
        return resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className);
    }


    public void testRegisterExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage("Function " + function + " already exist"), response.getEntity());
    }


    public void testRegisterFunctionUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = registerDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(createMessage("upload failure"), response.getEntity());
    }


    public void testRegisterFunctionSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultFunction();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }


    public void testRegisterFunctionFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("function failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }


    public void testRegisterFunctionInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(createMessage("java.io.IOException: Function registeration interrupted"), response.getEntity());
    }

    //
    // Update Functions
    //


    public void testUpdateFunctionMissingTenant() {
        testUpdateFunctionMissingArguments(
            null,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Tenant");
    }


    public void testUpdateFunctionMissingNamespace() {
        testUpdateFunctionMissingArguments(
            tenant,
            null,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Namespace");
    }


    public void testUpdateFunctionMissingFunctionName() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Function Name");
    }


    public void testUpdateFunctionMissingPackage() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Function Package");
    }


    public void testUpdateFunctionMissingPackageDetails() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            null,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Function Package");
    }


    public void testUpdateFunctionMissingSourceTopic() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            null,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            "Source Topic");
    }


    public void testUpdateFunctionMissingInputSerde() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            null,
            outputSerdeClassName,
            className,
            "InputSerdeClassName");
    }


    public void testUpdateFunctionMissingOutputSerde() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            null,
            className,
            "OutputSerdeClassName");
    }


    public void testUpdateFunctionMissingClassName() {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            null,
            "ClassName");
    }

    private void testUpdateFunctionMissingArguments(
        String tenant,
        String namespace,
        String function,
        InputStream inputStream,
        FormDataContentDisposition details,
        String sinkTopic,
        String sourceTopic,
        String inputSerdeClassName,
        String outputSerdeClassName,
        String className,
        String missingFieldName
    ) {
        Response response = resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response updateDefaultFunction() {
        return resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className);
    }


    public void testUpdateNotExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = updateDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage("Function " + function + " doesn't exist"), response.getEntity());
    }


    public void testUpdateFunctionUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Response response = updateDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(createMessage("upload failure"), response.getEntity());
    }


    public void testUpdateFunctionSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultFunction();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }


    public void testUpdateFunctionFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("function failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }


    public void testUpdateFunctionInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(createMessage("java.io.IOException: Function registeration interrupted"), response.getEntity());
    }

    //
    // deregister function
    //


    public void testDeregisterFunctionMissingTenant() throws Exception {
        testDeregisterFunctionMissingArguments(
            null,
            namespace,
            function,
            "Tenant");
    }


    public void testDeregisterFunctionMissingNamespace() throws Exception {
        testDeregisterFunctionMissingArguments(
            tenant,
            null,
            function,
            "Namespace");
    }


    public void testDeregisterFunctionMissingFunctionName() throws Exception {
        testDeregisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            "Function Name");
    }

    private void testDeregisterFunctionMissingArguments(
        String tenant,
        String namespace,
        String function,
        String missingFieldName
    ) {
        Response response = resource.deregisterFunction(
            tenant,
            namespace,
            function);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response deregisterDefaultFunction() {
        return resource.deregisterFunction(
            tenant,
            namespace,
            function);
    }


    public void testDeregisterNotExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage("Function " + function + " doesn't exist"), response.getEntity());
    }


    public void testDeregisterFunctionSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }


    public void testDeregisterFunctionFailure() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("function failed to deregister");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }


    public void testDeregisterFunctionInterrupted() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function deregisteration interrupted"));
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(createMessage("java.io.IOException: Function deregisteration interrupted"), response.getEntity());
    }

    //
    // Get Function Info
    //


    public void testGetFunctionMissingTenant() throws Exception {
        testGetFunctionMissingArguments(
            null,
            namespace,
            function,
            "Tenant");
    }


    public void testGetFunctionMissingNamespace() throws Exception {
        testGetFunctionMissingArguments(
            tenant,
            null,
            function,
            "Namespace");
    }


    public void testGetFunctionMissingFunctionName() throws Exception {
        testGetFunctionMissingArguments(
            tenant,
            namespace,
            null,
            "Function Name");
    }

    private void testGetFunctionMissingArguments(
        String tenant,
        String namespace,
        String function,
        String missingFieldName
    ) {
        Response response = resource.getFunctionInfo(
            tenant,
            namespace,
            function);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response getDefaultFunctionInfo() {
        return resource.getFunctionInfo(
            tenant,
            namespace,
            function);
    }


    public void testGetNotExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(createMessage("Function " + function + " doesn't exist"), response.getEntity());
    }


    public void testGetFunctionSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        FunctionMetaData metaData = new FunctionMetaData()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionConfig(new FunctionConfig()
                .setClassName(className)
                .setInputSerdeClassName(inputSerdeClassName)
                .setOutputSerdeClassName(outputSerdeClassName)
                .setName(function)
                .setNamespace(namespace)
                .setProcessingGuarantees(ProcessingGuarantees.ATMOST_ONCE)
                .setSinkTopic(sinkTopic)
                .setSourceTopic(sourceTopic)
                .setTenant(tenant))
            .setLimitsConfig(limitsConfig)
            .setPackageLocation(new PackageLocationMetaData().setPackagePath("/path/to/package"))
            .setRuntime("test")
            .setVersion(1234)
            .setWorkerId("test-worker");
        when(mockedManager.getFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(metaData.toJson(), response.getEntity());
    }

    //
    // List Functions
    //


    public void testListFunctionsMissingTenant() throws Exception {
        testListFunctionsMissingArguments(
            null,
            namespace,
            "Tenant");
    }


    public void testListFunctionsMissingNamespace() throws Exception {
        testListFunctionsMissingArguments(
            tenant,
            null,
            "Namespace");
    }


    private void testListFunctionsMissingArguments(
        String tenant,
        String namespace,
        String missingFieldName
    ) {
        Response response = resource.listFunctions(
            tenant,
            namespace);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response listDefaultFunctions() {
        return resource.listFunctions(
            tenant,
            namespace);
    }


    public void testListFunctionsSuccess() throws Exception {
        List<String> functions = Lists.newArrayList("test-1", "test-2");
        when(mockedManager.listFunction(eq(tenant), eq(namespace))).thenReturn(functions);

        Response response = listDefaultFunctions();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(functions), response.getEntity());
    }
}
