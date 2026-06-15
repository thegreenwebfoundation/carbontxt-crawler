// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.filtering;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URL;

import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CarbonTxtRedirectFilterTest {

    private CarbonTxtRedirectFilter filter;

    @BeforeEach
    void setUp() {
        filter = new CarbonTxtRedirectFilter();
    }

    @Test
    void filtersNonCarbonTxtOutlinkFromRedirectedCarbonTxt() throws Exception {
        URL source = new URL("https://example.com/carbon.txt");
        Metadata metadata = new Metadata();
        metadata.setValue("_redirTo", "https://www.example.com/carbon.txt");

        String result =
                filter.filter(source, metadata, "https://example.com/report/sustainability.pdf");

        assertNull(result, "Should filter out non-carbon.txt outlink from redirected carbon.txt");
    }

    @Test
    void allowsCarbonTxtOutlinkFromRedirectedCarbonTxt() throws Exception {
        URL source = new URL("https://example.com/carbon.txt");
        Metadata metadata = new Metadata();
        metadata.setValue("_redirTo", "https://www.example.com/carbon.txt");

        String target = "https://www.example.com/carbon.txt";
        String result = filter.filter(source, metadata, target);

        assertEquals(target, result, "Should allow carbon.txt outlink even when source redirects");
    }

    @Test
    void allowsOutlinkWhenSourceIsNotCarbonTxt() throws Exception {
        URL source = new URL("https://example.com/index.html");
        Metadata metadata = new Metadata();
        metadata.setValue("_redirTo", "https://www.example.com/index.html");

        String target = "https://example.com/about.html";
        String result = filter.filter(source, metadata, target);

        assertEquals(target, result, "Should not filter when source is not carbon.txt");
    }

    @Test
    void allowsOutlinkWhenNoRedirect() throws Exception {
        URL source = new URL("https://example.com/carbon.txt");
        Metadata metadata = new Metadata();

        String target = "https://example.com/report/sustainability.pdf";
        String result = filter.filter(source, metadata, target);

        assertEquals(target, result, "Should not filter when there is no _redirTo metadata");
    }

    @Test
    void allowsOutlinkWhenRedirToIsBlank() throws Exception {
        URL source = new URL("https://example.com/carbon.txt");
        Metadata metadata = new Metadata();
        metadata.setValue("_redirTo", "  ");

        String target = "https://example.com/report/sustainability.pdf";
        String result = filter.filter(source, metadata, target);

        assertEquals(target, result, "Should not filter when _redirTo is blank");
    }

    @Test
    void handlesNullInputsGracefully() throws Exception {
        URL source = new URL("https://example.com/carbon.txt");
        Metadata metadata = new Metadata();

        assertNull(filter.filter(source, metadata, null));
        assertEquals(
                "https://example.com/page",
                filter.filter(null, metadata, "https://example.com/page"));
        assertEquals(
                "https://example.com/page",
                filter.filter(source, null, "https://example.com/page"));
    }
}
