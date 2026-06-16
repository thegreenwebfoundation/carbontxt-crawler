// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.bolt;

import crawlercommons.domains.EffectiveTldFinder;
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
 * Listens to tuples on the status stream
 * Looks for `CarbonTxt-Location: URL` in the HTTP header.
 * Writes the newly discovered URLs to the status stream as usual.
 **/
public class HeaderDiscoveryBolt extends StatusEmitterBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderDiscoveryBolt.class);
    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;

    // typically "protocol."
    private String protocolMetadataPrefix;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url"));
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(conf, topologyContext, outputCollector);
        protocolMetadataPrefix = ConfUtils.getString(
                conf,
                ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM,
                "");
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata =
                (Metadata) tuple.getValueByField("metadata");
        Status status = (Status) tuple.getValueByField("status");

        LOG.info("Checking in http header for {}", url);

        // Look in HTTP headers metadata for CarbonTxt-Location (case-insensitive)
        String expectedKey = (protocolMetadataPrefix + "carbontxt-location").toLowerCase(Locale.ROOT);
        String carbonTxtLocation = metadata.getFirstValue(expectedKey);
        if (carbonTxtLocation != null) {
            if (carbonTxtLocation.startsWith("http")) {
                LOG.info("Found URL in http header for {} : {}", url, carbonTxtLocation);
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
                    collector.emit(_s, tuple, new Values(ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED));
                }
            }
            else {
                String domain = EffectiveTldFinder.getAssignedDomain(carbonTxtLocation, true); // strict=true
                if (domain != null) {
                    // pass the domain to the seed generator
                    LOG.info("Found domain in http header for  {} : {}", url, carbonTxtLocation);
                    collector.emit(tuple, new Values(carbonTxtLocation));
                }
            }
        }

        // pass it on
        collector.emit(_s, tuple, new Values(url, metadata, status));
        collector.ack(tuple);
    }

}
