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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Predicate;

public class IndexDataManager
{
    public IndexDataEntry load(Path indexPath, Predicate<Statistics> predicate)
            throws IOException
    {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(indexPath.toAbsolutePath().toString()),
                        new Configuration())).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
            return null;
        }
    }

    public static class IndexDataEntry
    {
        final Map<Long, Statistics> index;

        public IndexDataEntry(Map<Long, Statistics> index) {
            this.index = index;
        }
    }

    // assuming source always partitioned
    public static class Statistics
    {
        String partitionName;
        String minValue;
        String maxValue;
    }
}
