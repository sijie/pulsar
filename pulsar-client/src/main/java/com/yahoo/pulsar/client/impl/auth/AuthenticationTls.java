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
package com.yahoo.pulsar.client.impl.auth;

import java.io.IOException;
import java.util.Map;

import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.AuthenticationDataProvider;
import com.yahoo.pulsar.client.api.PulsarClientException;

/**
 * 
 * This plugin requires these parameters
 * 
 * tlsCertFile: A file path for a client certificate. tlsKeyFile: A file path for a client private key.
 *
 */
public class AuthenticationTls implements Authentication {

    private String certFilePath;
    private String keyFilePath;

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public String getAuthMethodName() {
        return "tls";
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        try {
            return new AuthenticationDataTls(certFilePath, keyFilePath);
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public void configure(Map<String, String> authParams) {
        certFilePath = authParams.get("tlsCertFile");
        keyFilePath = authParams.get("tlsKeyFile");
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
    }

}
