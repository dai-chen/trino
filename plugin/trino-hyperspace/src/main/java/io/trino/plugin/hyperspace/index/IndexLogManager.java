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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexLogManager
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path indexPath;

    public IndexLogManager(Path indexPath)
    {
        this.indexPath = indexPath;
    }

    public IndexLogEntry getLatestStableLog()
            throws IOException
    {
        String latestStableLogPath = indexPath.toString() + "/_hyperspace_log/latestStable";
        JsonNode root = MAPPER.readTree(new File(latestStableLogPath));

        Map<Long, String> indexIdTracker = new HashMap<>();
        parseContent(root.at("/content/root"), new ArrayList<>(), indexIdTracker);

        Map<Long, String> sourceIdTracker = new HashMap<>();
        root.at("/source/plan/properties/relations").forEach(relation ->
                parseContent(relation.at("/data/properties/content/root"), new ArrayList<>(), sourceIdTracker));

        return new IndexLogEntry(indexIdTracker, sourceIdTracker);
    }

    private void parseContent(JsonNode root, List<String> path, Map<Long, String> idTracker)
    {
        if (root == null) {
            return;
        }

        path.add(root.get("name").asText());
        root.get("files").forEach(file ->
                idTracker.put(file.get("id").asLong(), String.join("/", path) + "/" + file.get("name").asText()));
        root.get("subDirs").forEach(subDir -> parseContent(subDir, path, idTracker));
        path.remove(path.size() - 1);
    }

    public static class IndexLogEntry
    {
        final Map<Long, String> indexIdTracker; // file ID => index data file path
        final Map<Long, String> sourceIdTracker; // file ID => source data file path

        public IndexLogEntry(Map<Long, String> indexIdTracker, Map<Long, String> sourceIdTracker)
        {
            this.indexIdTracker = indexIdTracker;
            this.sourceIdTracker = sourceIdTracker;
        }
    }
}
