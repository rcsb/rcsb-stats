package org.rcsb.stats;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class Helpers {
    private static final Logger logger = LoggerFactory.getLogger(Helpers.class);

    /**
     * Delete a directory with all its contents.
     * @param dir the directory to delete
     * @throws IOException operation failed
     */
    public static void deleteDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }

        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    /**
     * Create a directory (and parents) as needed.
     * @param dir the directory to create.
     * @throws IOException operation failed
     */
    public static void createDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            return;
        }

        Files.createDirectories(dir);
    }

    /**
     * Download content from a URL to a file.
     * @param url source
     * @param destination destination
     * @throws IOException operation failed
     */
    public static void downloadFromUrl(URL url, Path destination) throws IOException {
        ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(destination.toFile());
        fileOutputStream.getChannel()
                .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
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
        logger.info("There are {} entries", out.size());
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
}
