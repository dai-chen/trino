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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Index log manager that read index metadata file to parse index data file location
 * and mappings from file ID to source data file.
 */
public class IndexLogManager
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path indexPath;

    public IndexLogManager(Path indexPath)
    {
        this.indexPath = indexPath;
    }

    /**
     * Parse latest stable log in index metadata folder.
     */
    public IndexLogEntry getLatestStableLog()
            throws IOException
    {
        String latestStableLogPath = indexPath.toString() + "/_hyperspace_log/latestStable";
        JsonNode root = MAPPER.readTree(new File(latestStableLogPath));

        Map<Long, String> indexIdTracker = parseContent(root.at("/content/root"));

        Map<Long, String> sourceIdTracker = new HashMap<>();
        root.at("/source/plan/properties/relations").forEach(relation ->
                sourceIdTracker.putAll(parseContent(relation.at("/data/properties/content/root"))));
        return new IndexLogEntry(indexIdTracker, sourceIdTracker);
    }

    private Map<Long, String> parseContent(JsonNode root)
    {
        Map<Long, String> idTracker = new HashMap<>();
        parseContent(root, new ArrayList<>(), idTracker);
        return idTracker;
    }

    private void parseContent(JsonNode root, List<String> path, Map<Long, String> idTracker)
    {
        if (root == null) {
            return;
        }

        String name = root.get("name").asText();
        if (name.startsWith("file:/")) {
            name = name.replaceFirst("/", "///"); // replace file:/ with file:/// because of no authority (host:port in URI)
        }
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1); // handle special case in s3a://.../
        }

        path.add(name);
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
