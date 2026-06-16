// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.greenwebfoundation.carbontxt.MetadataKeys.HOSTNAME;
import static org.greenwebfoundation.carbontxt.MetadataKeys.METHOD;

/**
 * Generates candidates URLS from hostnames.
 * e.g. xxx/carbon.txt
 * xxx/.well-known/carbon.txt
 * with a metadata 'method' reflecting the method
 * Emits them on the status stream
 */

public class SeedGenerator extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SeedGenerator.class);
    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;
    protected OutputCollector collector;

    public SeedGenerator() {
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {

        // we get a url from the FileSpout - it is just a string to represent
        // a hostname though but it does not matter really
        String hostname = input.getStringByField("url");

        LOG.info("Generating seeds for {}", hostname);

        // generate 2 candidate URLs for carbon.txt
        // assume a https prefix
        String candidate = "https://" + hostname + "/carbon.txt";
        sendCandidate(hostname, candidate, "root", input);

        String candidate2 = "https://" + hostname + "/.well-known/carbon.txt";
        sendCandidate(hostname, candidate2, "well-known", input);

        collector.ack(input);
    }

    private void sendCandidate(String hostname, String candidate, String methodValue, Tuple input) {
        Metadata metadata = new Metadata();
        metadata.setValue(HOSTNAME, hostname);
        metadata.setValue(METHOD, methodValue);
        Values v = new Values(candidate, metadata, Status.DISCOVERED);
        collector.emit(_s, input, v);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields f = new Fields("url", "metadata", "status");
        declarer.declareStream(_s, f);
    }

}
