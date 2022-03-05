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
package io.trino.index.dataskipping;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IndexDataManager
{
    private final TupleDomain<?> predicate;

    public IndexDataManager(TupleDomain<?> predicate)
    {
        this.predicate = predicate;
    }

    public Map<Long, IndexDataEntry> getIndexData(Path indexPath)
            throws IOException
    {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(indexPath.toString()),
                        new Configuration(false)))
                .build()) {
            Map<Long, IndexDataEntry> indexData = new HashMap<>();
            GenericRecord record;
            while ((record = reader.read()) != null) {
                long sourceFileId = (long) record.get("_data_file_id");
                long minValue = (long) record.get("MinMax_l_extendedprice__0"); // TODO: remove hardcoding name and type
                long maxValue = (long) record.get("MinMax_l_extendedprice__1");

                IndexDataEntry entry = new IndexDataEntry(sourceFileId, minValue, maxValue);
                if (isPredicateTrue(entry)) {
                    indexData.put(sourceFileId, entry);
                }
            }
            return indexData;
        }
    }

    private boolean isPredicateTrue(IndexDataEntry entry)
    {
        Domain range = Domain.create(ValueSet.ofRanges(
                Range.range(DecimalType.createDecimalType(12, 2), entry.minValue, true, entry.maxValue, true)), false);
        Domain expression = predicate.getDomains().get().entrySet().iterator().next().getValue();
        return !expression.intersect(range).isNone();
    }

    public static class IndexDataEntry
    {
        final long sourceFileId;
        final long minValue;
        final long maxValue;

        public IndexDataEntry(long sourceFileId, long minValue, long maxValue)
        {
            this.sourceFileId = sourceFileId;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexDataEntry that = (IndexDataEntry) o;
            return sourceFileId == that.sourceFileId && minValue == that.minValue && maxValue == that.maxValue;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceFileId, minValue, maxValue);
        }

        @Override
        public String toString()
        {
            return "IndexDataEntry{" +
                    "sourceFileId=" + sourceFileId +
                    ", minValue=" + minValue +
                    ", maxValue=" + maxValue +
                    '}';
        }
    }
}
