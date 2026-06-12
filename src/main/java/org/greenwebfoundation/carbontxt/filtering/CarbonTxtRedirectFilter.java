// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.filtering;

import java.net.URL;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters out a redirection from a carbon.txt page unless it is itself a carbon.txt.
 */
public class CarbonTxtRedirectFilter extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(CarbonTxtRedirectFilter.class);

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata, String urlToFilter) {
        if (sourceUrl == null || sourceMetadata == null || urlToFilter == null) {
            return urlToFilter;
        }

        // Only applies when the source is a carbon.txt page
        if (!sourceUrl.getPath().endsWith("/carbon.txt")) {
            return urlToFilter;
        }

        // Only applies when the parent has a redirect
        String redirTo = sourceMetadata.getFirstValue("_redirTo");
        if (redirTo == null || redirTo.isBlank()) {
            return urlToFilter;
        }

        // Allow the outlink if it is itself a carbon.txt
        if (urlToFilter.endsWith("/carbon.txt")) {
            return urlToFilter;
        }

        LOG.debug(
                "Filtering outlink {} from redirected carbon.txt {} (redirects to {})",
                urlToFilter,
                sourceUrl,
                redirTo);

        return null;
    }

    @Override
    public void configure(Map<String, Object> stormConf, JsonNode filterParams) {
        // No configuration needed
    }
}
