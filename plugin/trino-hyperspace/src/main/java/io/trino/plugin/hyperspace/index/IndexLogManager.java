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

        String logDataFilePath = parseContent(root.at("/content/root"));

        Map<String, String> idTracker = new HashMap<>();
        root.at("/source/plan/properties/relations").forEach(relation ->
                parseSource(relation.at("/data/properties/content/root"), new ArrayList<>(), idTracker));

        return new IndexLogEntry(logDataFilePath, idTracker);
    }

    // Assuming there is only one index data file
    private String parseContent(JsonNode root)
    {
        StringBuilder path = new StringBuilder();
        while (root != null) {
            path.append(root.get("name").asText()).append("/");
            root = root.path("subDirs").get(0);
        }
        return path.toString();
    }

    private void parseSource(JsonNode root, List<String> path, Map<String, String> idTracker)
    {
        if (root == null) {
            return;
        }

        path.add(root.get("name").asText());
        root.get("files").forEach(file ->
                idTracker.put(file.get("id").asText(), String.join("/", path) + "/" + file.get("name").asText()));
        root.get("subDirs").forEach(subDir -> parseSource(subDir, path, idTracker));
        path.remove(path.size() - 1);
    }

    public static class IndexLogEntry
    {
        String logDataFilePath;
        Map<String, String> sourceIdTracker; // file ID => source data file path

        public IndexLogEntry(String logDataFilePath, Map<String, String> sourceIdTracker)
        {
            this.logDataFilePath = logDataFilePath;
            this.sourceIdTracker = sourceIdTracker;
        }
    }

}
