package org.rcsb.stats;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.rcsb.cif.CifIO;
import org.rcsb.cif.schema.StandardSchemata;
import org.rcsb.cif.schema.mm.MmCifFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class Helpers {
    private static final Logger logger = LoggerFactory.getLogger(Helpers.class);

    /**
     * Obtain structure data for a collection of identifiers.
     * @param identifiers set to operate on
     * @return stream of structure data
     */
    public static Stream<MmCifFile> fetchStructureData(Collection<String> identifiers) {
        return identifiers.parallelStream()
                .map(i -> {
                    try {
                        return CifIO.readFromURL(new URL(String.format(Constants.BCIF_SOURCE, i))).as(StandardSchemata.MMCIF);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    /**
     * Get a list of all experimental IDs known to the production system.
     * @return collection of known entry IDs
     * @throws IOException operation failed
     */
    public static Set<String> getAllIdentifiers() throws IOException {
        URL url = getSearchUrl();
        logger.info("Retrieving current entry list from RCSB PDB Search API at {}", url.toString().split("\\?")[0]);
        Set<String> out = new HashSet<>();
        try (InputStream inputStream = url.openStream()) {
            JsonElement jsonElement = new Gson().fromJson(new InputStreamReader(inputStream), JsonElement.class);
            JsonObject jsonObject = jsonElement.getAsJsonObject();

            jsonObject.getAsJsonArray("result_set")
                    .forEach(id -> out.add(id.getAsString()));
        }
        logger.info("There are {} entries", Helpers.formatNumber(out.size()));
        return out;
    }

    private static URL getSearchUrl() throws MalformedURLException {
        String query = URLEncoder.encode("{\n" +
                "  \"query\": {\n" +
                "    \"type\": \"terminal\",\n" +
                "    \"label\": \"text\",\n" +
                "    \"service\": \"text\",\n" +
                "    \"parameters\": {\n" +
                "      \"attribute\": \"rcsb_entry_container_identifiers.entry_id\",\n" +
                "      \"operator\": \"exists\",\n" +
                "      \"negation\": false\n" +
                "    }\n" +
                "  },\n" +
                "  \"return_type\": \"entry\",\n" +
                "  \"request_options\": {\n" +
                "    \"results_content_type\": [\n" +
                "      \"experimental\"\n" +
                "    ],\n" +
                "    \"return_all_hits\": true,\n" +
                "    \"results_verbosity\": \"compact\"\n" +
                "  }\n" +
                "}", StandardCharsets.UTF_8);
        return new URL("https://search.rcsb.org/rcsbsearch/v2/query?json=" + query);
    }

    /**
     * Print an int nicely.
     * @param i value
     * @return String with thousands separator
     */
    public static String formatNumber(int i) {
        return String.format("%,d", i);
    }

    /**
     * Print a float nicely.
     * @param d value
     * @return String with thousands separator and 2 decimal places
     */
    public static String formatNumber(double d) {
        return String.format("%,.2f", d);
    }
}
