// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.bolt;

import crawlercommons.domains.EffectiveTldFinder;
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

import static org.greenwebfoundation.carbontxt.MetadataKeys.HOSTNAME;
import static org.greenwebfoundation.carbontxt.MetadataKeys.METHOD;

/**
 * Get tuples from the status stream
 * Look for DNS TXT record in the format carbon-txt-location=URL
 * The latter is tried only for seeds with method value = root
 * Writes the newly discovered URLs to the status stream as usual.
 **/
public class DNSDiscoveryBolt extends StatusEmitterBolt {

    private static final Logger LOG = LoggerFactory.getLogger(DNSDiscoveryBolt.class);
    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url"));
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata =
                (Metadata) tuple.getValueByField("metadata");
        Status status = (Status) tuple.getValueByField("status");

        // get the hostname from the metadata
        final String method = metadata.getFirstValue(METHOD);
        final String hostname = metadata.getFirstValue(HOSTNAME);

        // don't want to trigger this more than once per hostname
        // do it only when the seed has method root
        if ("root".equalsIgnoreCase(method) && hostname != null) {

            LOG.info("Checking in DNS record for {}", url);

            try {
                Lookup lookup = new Lookup(hostname, Type.TXT);
                Record[] records = lookup.run();
                if (records != null) {
                    for (Record record : records) {
                        if (record instanceof TXTRecord txt) {
                            for (String str : txt.getStrings()) {
                                String key = "carbon-txt-location=";
                                if (str.startsWith(key)) {
                                    String value = str.substring(key.length());
                                    // check that the value is a valid URL

                                    // if not, check whether it is a hostname
                                    // in which case we'd trigger a new seed generation
                                    if (value.startsWith("http")) {
                                        LOG.info("Found URL in DNS TXT record for {} : {}", url, value);
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
                                            collector.emit(_s, tuple, new Values(ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED));
                                        }
                                    } else {
                                        String domain = EffectiveTldFinder.getAssignedDomain(value, true); // strict=true
                                        if (domain != null) {
                                            // pass the domain to the seed generator
                                            LOG.info("Found domain in DNS TXT record for {} : {}", url, value);
                                            collector.emit(tuple, new Values(value));
                                        }
                                    }
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
        collector.emit(_s, tuple, new Values(url, metadata, status));
        collector.ack(tuple);
    }

}
