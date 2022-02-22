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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * +-------------+-------------------+------------------+------------------+
 * |_data_file_id|Partition_region__0|MinMax_orderkey__0|MinMax_orderkey__1|
 * +-------------+-------------------+------------------+------------------+
 * |            2|                 US|                 1|                 1|
 * |            1|                 US|                 2|                 2|
 * |            0|                 US|                 4|                 4|
 * |            3|                 EU|                 3|                 3|
 * +-------------+-------------------+------------------+------------------+
*/
public class TestHyperspaceDataSkippingIndex
{
    private static final Path INDEX_ROOT_PATH = Path.of("src/test/resources/orders-minmax").toAbsolutePath();

    private final static ColumnHandle COLUMN_HANDLE = new ColumnHandle() {};

    @Test
    public void testIsSplitIncludedWithEqualityPredicate()
    {
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(Range.equal(BigintType.BIGINT, 2L)), false)));
        HyperspaceDataSkippingIndex index = new HyperspaceDataSkippingIndex(INDEX_ROOT_PATH, predicate);

        assertFalse(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=US/part-00001-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
        assertTrue(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=US/part-00002-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
        assertFalse(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=US/part-00003-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
        assertFalse(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=EU/part-00000-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
    }

    @Test
    public void testIsSplitIncludedWithRangePredicate()
    {
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(Range.greaterThan(BigintType.BIGINT, 1L)), false)));
        HyperspaceDataSkippingIndex index = new HyperspaceDataSkippingIndex(INDEX_ROOT_PATH, predicate);

        assertFalse(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=US/part-00001-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
        assertTrue(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=US/part-00002-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
        assertTrue(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=US/part-00003-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
        assertTrue(index.isDataFileIncluded(Path.of("file://Users/daichen/Temp/orders/region=EU/part-00000-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet")));
    }
}
