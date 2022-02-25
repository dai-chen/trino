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
package io.trino.plugin.hyperspace.index;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static io.trino.plugin.hyperspace.index.IndexLogManager.IndexLogEntry;
import static org.testng.Assert.assertEquals;

public class TestIndexLogManager
{
    private final Path indexPath = new File("src/test/resources/orders-minmax").toPath();

    private final IndexLogManager indexLogManager = new IndexLogManager(indexPath);

    @Test
    public void testGetLatestStableLog()
            throws IOException
    {
        IndexLogEntry latestStableLog = indexLogManager.getLatestStableLog();
        assertEquals(latestStableLog.indexIdTracker, ImmutableMap.of(
                4L,
                "/Users/daichen/Software/spark-3.1.2-bin-hadoop3.2/spark-warehouse/indexes/orders-minmax/v__=0/part-00000-0ddc1a94-ade4-48b6-910a-3c521a415aa4-c000.snappy.parquet"));
        assertEquals(latestStableLog.sourceIdTracker, ImmutableMap.of(
                0L, "/Users/daichen/Temp/orders/region=US/part-00003-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet",
                1L, "/Users/daichen/Temp/orders/region=US/part-00002-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet",
                2L, "/Users/daichen/Temp/orders/region=US/part-00001-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet",
                3L, "/Users/daichen/Temp/orders/region=EU/part-00000-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet"));
    }
}
