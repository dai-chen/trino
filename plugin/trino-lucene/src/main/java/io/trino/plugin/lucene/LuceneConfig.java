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
package io.trino.plugin.lucene;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class LuceneConfig
{
    private URI metadata;
    private int splitsPerIndex;

    @NotNull
    public URI getMetadata()
    {
        return metadata;
    }

    @Config("lucene.metadata-uri")
    public LuceneConfig setMetadata(URI metadata)
    {
        this.metadata = metadata;
        return this;
    }

    public int getSplitsPerIndex()
    {
        return splitsPerIndex;
    }

    @Config("lucene.splits-per-index")
    public LuceneConfig setSplitsPerIndex(int splitsPerIndex)
    {
        this.splitsPerIndex = splitsPerIndex;
        return this;
    }
}
