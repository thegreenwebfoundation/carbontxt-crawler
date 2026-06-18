/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.opensearch.persistence;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSpout extends AbstractSpout {

    private static final Logger LOG = LoggerFactory.getLogger(QueueSpout.class);

    private static final String OSBoltType = "queues";
    private static final String OSQueueIndexNameParamName =
            Constants.PARAMPREFIX + OSBoltType + ".index.name";

    private int maxQueueUrls = 100;
    private int pageSize = 50;

    @Override
    public void open(
            Map<String, Object> stormConf,
            TopologyContext context,
            SpoutOutputCollector collector) {

        super.open(stormConf, context, collector);

        // Override status indexName with the queues index name
        indexName = ConfUtils.getString(stormConf, OSQueueIndexNameParamName, "queues");

        // Custom QueueSpout limits
        maxQueueUrls = ConfUtils.getInt(stormConf, "opensearch.queues.max.urls", 100);
        pageSize = ConfUtils.getInt(stormConf, "opensearch.queues.page.size", 50);

        synchronized (AbstractSpout.class) {
            try {
                IndexCreation.checkOrCreateIndex(client, indexName, OSBoltType, LOG);
            } catch (IOException e) {
                LOG.error("{} Can't check or create index {}", logIdprefix, indexName, e);
                throw new RuntimeException(e);
            }
        }

        LOG.info("{} QueueSpout opened. indexName: {}, maxQueueUrls: {}, pageSize: {}",
                logIdprefix, indexName, maxQueueUrls, pageSize);
    }

    @Override
    protected void populateBuffer() {
        if (client == null) {
            LOG.error("{} OpenSearch client is not initialized", logIdprefix);
            return;
        }

        LOG.debug("{} Populating buffer from index {}", logIdprefix, indexName);

        int from = 0;
        boolean hasMore = true;
        int addedCount = 0;

        // Fetch everything due up to now
        String nowStr = Instant.now().toString();

        while (hasMore && buffer.size() < maxQueueUrls) {
            SearchRequest searchRequest = new SearchRequest(indexName);

            // Shard pinning for multi-instance correctness
            if (shardID != -1) {
                searchRequest.preference("_shards:" + shardID);
            }

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.rangeQuery("nextFetchDate").lte(nowStr));
            sourceBuilder.sort("nextFetchDate", SortOrder.ASC);
            sourceBuilder.from(from);
            sourceBuilder.size(pageSize);

            if (queryTimeout > 0) {
                sourceBuilder.timeout(new TimeValue(queryTimeout, TimeUnit.SECONDS));
            }

            searchRequest.source(sourceBuilder);

            try {
                SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                SearchHit[] hits = response.getHits().getHits();

                if (hits == null || hits.length == 0) {
                    hasMore = false;
                    break;
                }

                for (SearchHit hit : hits) {
                    Map<String, Object> keyValues = hit.getSourceAsMap();
                    if (keyValues == null) {
                        continue;
                    }

                    // Handles both 'url' and 'key' attributes gracefully
                    String url = (String) keyValues.get("url");
                    if (url == null) {
                        url = (String) keyValues.get("key");
                    }

                    if (url == null) {
                        LOG.warn("{} Missing key/url in document {}", logIdprefix, hit.getId());
                        continue;
                    }

                    // Skip URLs currently being processed by the topology
                    if (beingProcessed.containsKey(url)) {
                        LOG.debug("{} URL is already in-flight, skipping: {}", logIdprefix, url);
                        continue;
                    }

                    Metadata metadata = fromKeyValues(keyValues);
                    if (buffer.add(url, metadata)) {
                        addedCount++;
                    }

                    if (buffer.size() >= maxQueueUrls) {
                        hasMore = false;
                        break;
                    }
                }

                // If page response has fewer documents than requested, we are done
                if (hits.length < pageSize) {
                    hasMore = false;
                } else {
                    from += pageSize;
                }

            } catch (IOException e) {
                LOG.error(logIdprefix + " Exception querying OpenSearch for queue items", e);
                hasMore = false;
            }
        }

        LOG.debug("{} Buffer population complete. Added {} items. Current buffer size: {}",
                logIdprefix, addedCount, buffer.size());
    }

    protected final Metadata fromKeyValues(Map<String, Object> keyValues) {
        Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues.get("metadata");
        Metadata metadata = new Metadata();
        // TODO proper handling of metadata
        return metadata;
    }

}
