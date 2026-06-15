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
import org.apache.stormcrawler.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.*;
import org.xbill.DNS.Record;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import static org.greenwebfoundation.carbontxt.MetadataKeys.HOSTNAME;
import static org.greenwebfoundation.carbontxt.MetadataKeys.METHOD;

/**
 * Look for DNS TXT record in the format carbon-txt-location=URL
 * The latter is tried only for seeds with method value = root
 * Writes the newly discovered URLs to the status stream as usual.
 **/
public class DNSDiscoveryBolt extends StatusEmitterBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DNSDiscoveryBolt.class);
    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(conf, topologyContext, outputCollector);
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

        // get the hostname from the metadata
        final String method = metadata.getFirstValue(METHOD);
        final String hostname = metadata.getFirstValue(HOSTNAME);

        // don't want to trigger this more than once per hostname
        // do it only when the seed has method root
        if ("root".equalsIgnoreCase(method) && hostname != null){
            try {
                Lookup lookup = new Lookup(hostname, Type.TXT);
                Record[] records = lookup.run();
                for (Record record : records) {
                    if (record instanceof TXTRecord) {
                        TXTRecord txt = (TXTRecord) record;
                        for (String str : txt.getStrings()) {
                            String key = "carbon-txt-location=";
                            if (str.startsWith(key)) {
                                String value = str.substring(key.length());
                                LOG.info("Found match in DNS TXT record for {} : {}", url, value);
                                URL sourceURL;
                                try {
                                    sourceURL = URLUtil.toURL(url);
                                } catch (MalformedURLException e) {
                                    // should not happen
                                    throw new RuntimeException(e);
                                }
                                Outlink ol = filterOutlink(sourceURL, value, metadata);
                                // override the value
                                if (ol != null) {
                                    ol.getMetadata().setValue(METHOD, "dns");
                                    collector.emit(_s, tuple, new Values(new Object[]{ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED}));
                                }
                            }
                        }
                    }
                }
            } catch (TextParseException e) {
                LOG.error("Error for {}: {}", url, e.getMessage());
            }
        }

        // pass it on
        collector.emit(tuple, new Values(url, metadata, Status.FETCHED));
        collector.ack(tuple);
    }

}
