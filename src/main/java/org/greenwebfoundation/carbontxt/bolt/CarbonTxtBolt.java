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
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.Map;

import static org.greenwebfoundation.carbontxt.MetadataKeys.*;

/**
 * Triggered only if a document has been fetched.
 * Checks that the document is a valid toml and if so send it for indexing
 * otherwise, update its status as error
 */
public class CarbonTxtBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CarbonTxtBolt.class);

    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        byte[] content = input.getBinaryByField("content");
        Metadata metadata =
                (Metadata) input.getValueByField("metadata");

        LOG.info("Processing carbon.txt: {}", url);
        boolean valid = true;

        if (content == null || content.length == 0) {
            valid = false;
            metadata.addValue(ERRORS, "Empty content");
        }

        if (valid) {
            TomlParseResult toml = null;
            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content)) {
                toml = Toml.parse(byteArrayInputStream);
            } catch (Exception e) {
                valid = false;
                LOG.debug("Failed to parse TOML for {}: {}", url, e.getMessage());
                metadata.addValue(ERRORS, "Invalid TOML: " + e.getMessage());
            }

            if (toml != null && toml.hasErrors()) {
                valid = false;
                toml.errors().forEach(error -> metadata.addValue(ERRORS, error.toString()));
            }

            if (toml != null && toml.isEmpty()) {
                valid = false;
                metadata.addValue(ERRORS, "Empty TOML");
            }
        }

        metadata.setValue(VALID, Boolean.toString(valid));

        // nothing we want to do with it if invalid - just pass to status stream
        if (!valid) {
            LOG.info("Invalid carbon.txt at {}", url);
            collector.emit(_s, input, new Values(url, metadata, Status.ERROR));
            collector.ack(input);
            return;
        }

        // Send the document for indexing
        LOG.info("Valid carbon.txt at {}", url);
        // store content as base64
        // it is declared as type binary in schema
        byte[] encodedBytes = Base64.getEncoder().encode(content);
        metadata.setValue(CONTENT, new String(encodedBytes));
        collector.emit(input, new Values(url, content, metadata));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // when sending invalid to the status index
        declarer.declareStream(_s, new Fields("url", "metadata", "status"));
        // Default stream passes non-carbon.txt tuples to the parser bolt
        declarer.declare(new Fields("url", "content", "metadata"));
    }
}
