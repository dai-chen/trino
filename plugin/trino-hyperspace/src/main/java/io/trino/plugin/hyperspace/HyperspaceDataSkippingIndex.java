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
package io.trino.plugin.hyperspace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hyperspace data skipping index that skips split if possible.
 */
public class HyperspaceDataSkippingIndex {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String INDEX_REPOSITORY = "/Users/daichen/Software/spark-3.1.2-bin-hadoop3.2/spark-warehouse/indexes";

    private final Set<String> includedDataFiles;

    /**
     * @param indexName index name
     * @param predicate predicate on the indexed column
     */
    public HyperspaceDataSkippingIndex(String indexName, TupleDomain<? extends ColumnHandle> predicate) {
        try {
            IndexLogEntry indexLogEntry = loadIndexLog(indexName);
            List<IndexDataEntry> indexDataEntries = loadIndexData(indexLogEntry.logDataFilePath);
            this.includedDataFiles = skipSourceDataFiles(indexLogEntry, indexDataEntries, predicate);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public boolean isSplitIncluded(String filePath) {
        return includedDataFiles.contains(filePath);
    }

    private IndexLogEntry loadIndexLog(String indexName)
            throws IOException {
        String indexPath = INDEX_REPOSITORY + "/orders-minmax";
        String latestStableLogPath = indexPath + "/_hyperspace_log/1";
        JsonNode jsonNode = MAPPER.readTree(new File(latestStableLogPath));

        // Assuming there is only one index data file
        JsonNode root = jsonNode.at("/content/root");
        StringBuilder path = new StringBuilder();
        while (root != null) {
            path.append(root.get("name").asText()).append("/");
            root = root.path("subDirs").get(0);
        }
        return new IndexLogEntry(path.toString(), null);
    }

    private List<IndexDataEntry> loadIndexData(String indexDataPath)
    {
        //AvroParquetReader
        return null;
    }

    private void parseIndexLogEntry(JsonNode node, List<IndexDataEntry> results)
    {
    }

    private Set<String> skipSourceDataFiles(IndexLogEntry indexLogEntry, List<IndexDataEntry> indexDataEntries,
                                            TupleDomain<? extends ColumnHandle> predicate)
    {
        return indexDataEntries.stream()
                .map(index -> new SourceIndex(
                        indexLogEntry.sourceIdTracker.get(index.dataFileId),
                        index.minValue, index.maxValue))
                .filter(sourceIndex -> evaluatePredicate(predicate, sourceIndex))
                .map(sourceIndex -> sourceIndex.sourceDataFilePath)
                .collect(Collectors.toSet());
    }

    // ((Domain) ((Collections.UnmodifiableMap.UnmodifiableEntrySet.UnmodifiableEntry)((Collections.UnmodifiableMap)predicate.domains.value).entrySet().toArray()[1]).getValue())
    // .contains(Domain.singleValue(BigintType.BIGINT, 1L))
    private boolean evaluatePredicate(TupleDomain<? extends ColumnHandle> predicate, SourceIndex sourceIndex)
    {
        long minValue = Long.parseLong(sourceIndex.minValue);
        long maxValue = Long.parseLong(sourceIndex.maxValue);
        Domain range = Domain.create(ValueSet.ofRanges(Range.range(BigintType.BIGINT, minValue, true, maxValue, true)), false);
        Domain expression = predicate.getDomains().get().entrySet().iterator().next().getValue();
        return !expression.intersect(range).isNone();
    }

    static class IndexLogEntry
    {
        String logDataFilePath;
        Map<String, String> sourceIdTracker; // file ID => source data file path

        public IndexLogEntry(String logDataFilePath, Map<String, String> sourceIdTracker)
        {
            this.logDataFilePath = logDataFilePath;
            this.sourceIdTracker = sourceIdTracker;
        }
    }

    static class IndexDataEntry
    {
        String dataFileId;
        String partitionName;
        String minValue;
        String maxValue;
    }

    static class SourceIndex
    {
        String sourceDataFilePath;
        String minValue;
        String maxValue;

        SourceIndex(String sourceDataFilePath, String minValue, String maxValue)
        {
            this.sourceDataFilePath = sourceDataFilePath;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }
    }
}
