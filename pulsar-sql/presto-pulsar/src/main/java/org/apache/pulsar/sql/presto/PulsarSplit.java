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
package org.apache.pulsar.sql.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PulsarSplit implements ConnectorSplit {

    private final long splitId;
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final long splitSize;
    private final SchemaInfo schemaInfo;
    private final long startPositionEntryId;
    private final long endPositionEntryId;
    private final long startPositionLedgerId;
    private final long endPositionLedgerId;
    private final TupleDomain<ColumnHandle> tupleDomain;

    private final PositionImpl startPosition;
    private final PositionImpl endPosition;

    @JsonCreator
    public PulsarSplit(
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("splitSize") long splitSize,
            @JsonProperty("schema") SchemaInfo schemaInfo,
            @JsonProperty("startPositionEntryId") long startPositionEntryId,
            @JsonProperty("endPositionEntryId") long endPositionEntryId,
            @JsonProperty("startPositionLedgerId") long startPositionLedgerId,
            @JsonProperty("endPositionLedgerId") long endPositionLedgerId,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {
        this.splitId = splitId;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.splitSize = splitSize;
        this.schemaInfo = schemaInfo;
        this.startPositionEntryId = startPositionEntryId;
        this.endPositionEntryId = endPositionEntryId;
        this.startPositionLedgerId = startPositionLedgerId;
        this.endPositionLedgerId = endPositionLedgerId;
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.startPosition = PositionImpl.get(startPositionLedgerId, startPositionEntryId);
        this.endPosition = PositionImpl.get(endPositionLedgerId, endPositionEntryId);
    }

    @JsonProperty
    public long getSplitId() {
        return splitId;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public long getSplitSize() {
        return splitSize;
    }

    @JsonProperty
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @JsonProperty
    public long getStartPositionEntryId() {
        return startPositionEntryId;
    }

    @JsonProperty
    public long getEndPositionEntryId() {
        return endPositionEntryId;
    }

    @JsonProperty
    public long getStartPositionLedgerId() {
        return startPositionLedgerId;
    }

    @JsonProperty
    public long getEndPositionLedgerId() {
        return endPositionLedgerId;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    public PositionImpl getStartPosition() {
        return startPosition;
    }

    public PositionImpl getEndPosition() {
        return endPosition;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of(HostAddress.fromParts("localhost", 12345));
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public String toString() {
        return "PulsarSplit{" +
                "splitId=" + splitId +
                ", connectorId='" + connectorId + '\'' +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", splitSize=" + splitSize +
                ", schemaInfo='" + schemaInfo + '\'' +
                ", startPositionEntryId=" + startPositionEntryId +
                ", endPositionEntryId=" + endPositionEntryId +
                ", startPositionLedgerId=" + startPositionLedgerId +
                ", endPositionLedgerId=" + endPositionLedgerId +
                '}';
    }
}
