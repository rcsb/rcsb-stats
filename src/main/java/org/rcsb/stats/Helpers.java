package org.rcsb.stats;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.rcsb.cif.CifIO;
import org.rcsb.cif.ParsingException;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
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
                .map(identifier -> fetchStructureData(identifier, 0));
    }

    private static MmCifFile fetchStructureData(String identifier, int i) {
        try {
            return CifIO.readFromURL(new URL(String.format(Constants.BCIF_SOURCE, identifier))).as(StandardSchemata.MMCIF);
        } catch (IOException | ParsingException e) {
            logger.warn("Failed download of {} -- retrying", identifier);
            if (i < 3) {
                // TODO prolly should exponentially backoff this
                return fetchStructureData(identifier, i + 1);
            } else {
                if (e instanceof IOException f) {
                    throw new UncheckedIOException(f);
                } else {
                    throw (ParsingException) e;
                }
            }
        }
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
        String query = URLEncoder.encode(Constants.GET_ALL_QUERY, StandardCharsets.UTF_8);
        return new URL(Constants.SEARCH_API_URL + query);
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
     * Print an long nicely.
     * @param l value
     * @return String with thousands separator
     */
    public static String formatNumber(long l) {
        return String.format("%,d", l);
    }

    /**
     * Print a float nicely.
     * @param d value
     * @return String with thousands separator and 2 decimal places
     */
    public static String formatNumber(double d) {
        return String.format("%,.2f", d);
    }

    public static void updateCount(Class<?> task, long result, int structureCount) throws IOException {
        Path path = Paths.get("README.md");
        logger.info("Updating file at {}", path);
        String taskTag = task.getSimpleName().split("_")[0];
        String taskDescription = task.getSimpleName().split("_")[1];
        String out = Files.lines(path)
                .map(line -> {
                    if (line.startsWith("| ")) {
                        if (line.startsWith("| " + taskTag)) {
                            return "| " + taskTag + " | " + insertWhitespaceBeforeUpperCase(taskDescription) + " | " + formatNumber(result) + " |";
                        } else {
                            return line;
                        }
                    } else if (line.startsWith("Last updated")) {
                        LocalDate currentDate = LocalDate.now();
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy");
                        return line.split(": ")[0] + ": " + currentDate.format(formatter);
                    } else if (line.startsWith("Number of structures")) {
                        return line.split(": ")[0] + ": "  + formatNumber(structureCount);
                    } else {
                        return line;
                    }
                })
                .collect(Collectors.joining(System.lineSeparator()));
        Files.writeString(path, out);
    }

    private static String insertWhitespaceBeforeUpperCase(String input) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (i > 0 && Character.isUpperCase(c)) {
                sb.append(" ");
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
