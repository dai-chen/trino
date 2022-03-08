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

            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            IndexDataManager indexDataManager = new IndexDataManager(predicate);
            for (Map.Entry<Long, String> entry : indexLogEntry.indexIdTracker.entrySet()) {
                URI indexFilePath = new URI(entry.getValue());
                indexDataManager.getIndexData(indexFilePath).keySet().stream()
                        .map(indexLogEntry.sourceIdTracker::get)
                        .forEach(builder::add);
            }
            includedDataFiles = builder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isDataFileIncluded(String dataFilePath)
    {
        return includedDataFiles.contains(dataFilePath);
    }

    public Set<String> getAllIncludeDataFiles()
    {
        return includedDataFiles;
    }
}
