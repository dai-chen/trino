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

import com.google.common.collect.ImmutableSet;
import io.trino.index.dataskipping.sketch.BloomFilterSketch;
import io.trino.index.dataskipping.sketch.MinMaxSketch;
import io.trino.index.dataskipping.sketch.Sketch;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static io.trino.index.dataskipping.IndexLogManager.IndexLogEntry;

/**
 * Hyperspace data skipping index that skips split if possible.
 */
public class DataSkippingIndex
{
    private final Set<Long> includedPartitionNames;

    private final Set<String> includedDataFiles;

    /**
     * @param indexRootPath index root folder path
     * @param predicate predicate on the indexed column
     */
    public DataSkippingIndex(Path indexRootPath, TupleDomain<?> predicate)
    {
        try {
            IndexLogManager indexLogManager = new IndexLogManager(indexRootPath);
            IndexLogEntry indexLogEntry = indexLogManager.getLatestStableLog();
            Sketch sketch = createSketch(indexRootPath, predicate);

            ImmutableSet.Builder<Long> partitionNames = ImmutableSet.builder();
            ImmutableSet.Builder<String> dataFiles = ImmutableSet.builder();
            IndexDataManager indexDataManager = new IndexDataManager(sketch);
            for (Map.Entry<Long, String> entry : indexLogEntry.indexIdTracker.entrySet()) {
                URI indexFilePath = new URI(entry.getValue());
                IndexDataManager.IndexData indexData = indexDataManager.getIndexData(indexFilePath);
                partitionNames.addAll(indexData.partitionNames);
                indexData.dataFileIds.stream()
                        .map(indexLogEntry.sourceIdTracker::get)
                        .forEach(dataFiles::add);
            }

            includedPartitionNames = partitionNames.build();
            includedDataFiles = dataFiles.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isDataFileIncluded(String dataFilePath)
    {
        return includedDataFiles.contains(dataFilePath);
    }

    public Set<Long> getIncludedPartitionNames()
    {
        return includedPartitionNames;
    }

    public Set<String> getAllIncludeDataFiles()
    {
        return includedDataFiles;
    }

    private Sketch createSketch(Path indexRootPath, TupleDomain<?> predicate)
    {
        if (indexRootPath.toString().contains("minmax")) { // TODO: read type from index metadata
            return new MinMaxSketch(predicate);
        }
        return new BloomFilterSketch(predicate);
    }
}
