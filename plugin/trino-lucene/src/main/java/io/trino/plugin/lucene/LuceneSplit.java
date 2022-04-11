/*
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
package io.trino.plugin.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LuceneSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final URI path;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;
    private final int partNumber;
    private final int totalParts;

    @JsonCreator
    public LuceneSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("path") URI path,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partNumber") Integer partNumber,
            @JsonProperty("totalParts") Integer totalParts)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.path = requireNonNull(path, "path is null");
        this.addresses = requireNonNull(addresses, "address list is null");
        this.partNumber = requireNonNull(partNumber, "partNumber is null");
        this.totalParts = requireNonNull(totalParts, "totalParts is null");

//        if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
        remotelyAccessible = true;
        // addresses = ImmutableList.of(HostAddress.fromUri(uri));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public URI getPath()
    {
        return path;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
