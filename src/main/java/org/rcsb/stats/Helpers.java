package org.rcsb.stats;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared functionality.
 */
public class Helpers {
    private static final Logger logger = LoggerFactory.getLogger(Helpers.class);

    /**
     * Get a list of all experimental IDs known to the RCSB PDB Search API.
     * @param contentTypes flavor of identifiers to request
     * @return collection of known entry IDs
     * @throws IOException operation failed
     */
    public static Set<String> getAllIdentifiers(Constants.ResultsContentType... contentTypes) throws IOException {
        URL url = getSearchUrl(Set.of(contentTypes));
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

    private static URL getSearchUrl(Set<Constants.ResultsContentType> contentTypes) throws MalformedURLException {
        String ct = contentTypes.stream()
                .map(Constants.ResultsContentType::name)
                .map(String::toLowerCase)
                .map(t -> "\"" + t + "\"")
                .collect(Collectors.joining(", "));
        String query = URLEncoder.encode(String.format(Constants.GET_ALL_IDENTIFIERS_QUERY, ct), StandardCharsets.UTF_8);
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

    /**
     * Write output of a file to the Markdown table in README.md
     * @param task name of updater
     * @param result obtained count
     * @param structureCount number of evaluated entries
     * @throws IOException things went wrong
     */
    public static void updateCount(Class<?> task, long result, int structureCount) throws IOException {
        Path path = Paths.get("README.md");
        logger.info("Updating file at {}", path);
        String taskTag = task.getSimpleName().split("_")[0];
        String taskDescription = task.getSimpleName().split("_")[1];

        try (Stream<String> lines = Files.lines(path)) {
            String out = lines.map(line -> {
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
