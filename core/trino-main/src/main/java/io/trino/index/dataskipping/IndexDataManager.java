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

import io.trino.index.dataskipping.sketch.Sketch;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Index data manager that reads index data file to get the index content.
 */
public class IndexDataManager
{
    private final Sketch sketch;

    public IndexDataManager(Sketch sketch)
    {
        this.sketch = sketch;
    }

    public List<Long> getIndexData(URI indexPath)
            throws IOException
    {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(indexPath),
                        new Configuration(false)))
                .build()) {
            List<Long> indexData = new ArrayList<>();
            GenericRecord record;
            while ((record = reader.read()) != null) {
                long sourceFileId = (long) record.get("_data_file_id");
                if (sketch.evaluate(record)) {
                    indexData.add(sourceFileId);
                }
            }
            return indexData;
        }
    }
}
