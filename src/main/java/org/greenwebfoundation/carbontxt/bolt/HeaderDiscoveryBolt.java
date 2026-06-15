// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.StatusEmitterBolt;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;

import static org.greenwebfoundation.carbontxt.MetadataKeys.METHOD;

/**
 * Looks for `CarbonTxt-Location: URL` in the HTTP header.
 * Writes the newly discovered URLs to the status stream as usual.
 **/
public class HeaderDiscoveryBolt extends StatusEmitterBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderDiscoveryBolt.class);
    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;

    // typically "protocol."
    private String protocolMetadataPrefix;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(conf, topologyContext, outputCollector);
        protocolMetadataPrefix = ConfUtils.getString(
                conf,
                ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM,
                "");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        // Default stream passes non-carbon.txt tuples to the parser bolt
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata =
                (Metadata) tuple.getValueByField("metadata");

        // Look in HTTP headers metadata for CarbonTxt-Location (case-insensitive)
        String expectedKey = (protocolMetadataPrefix + "carbontxt-location").toLowerCase(Locale.ROOT);
        String carbonTxtLocation = metadata.getFirstValue(expectedKey);
        if (carbonTxtLocation != null) {
            LOG.info("Found match in http header for {} : {}", url, carbonTxtLocation);
            URL sourceURL;
            try {
                sourceURL = URLUtil.toURL(url);
            } catch (MalformedURLException e) {
                // should not happen
                throw new RuntimeException(e);
            }
            Outlink ol = filterOutlink(sourceURL, carbonTxtLocation, metadata);
            // override the value
            if (ol != null) {
                ol.getMetadata().setValue(METHOD, "http");
                collector.emit(_s, tuple, new Values(new Object[]{ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED}));
            }
        }

        collector.emit(tuple, new Values(url, metadata, Status.FETCHED));
        collector.ack(tuple);
    }

}
