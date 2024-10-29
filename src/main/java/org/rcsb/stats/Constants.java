package org.rcsb.stats;

/**
 * Configuration and constants.
 */
public class Constants {
    public static final String BCIF_SOURCE = "https://models.rcsb.org/%s.bcif.gz";
    public static final String SEARCH_API_URL = "https://search.rcsb.org/rcsbsearch/v2/query?json=";

    /**
     * Distinguishes experimentally-determined structures and computed structure models.
     */
    public enum ResultsContentType {
        EXPERIMENTAL, COMPUTATIONAL;
    }

    public static final String GET_ALL_IDENTIFIERS_QUERY = """
            {
              "query": {
                "type": "terminal",
                "label": "text",
                "service": "text",
                "parameters": {
                  "attribute": "rcsb_entry_container_identifiers.entry_id",
                  "operator": "exists",
                  "negation": false
                }
              },
              "return_type": "entry",
              "request_options": {
                "results_content_type": [%s],
                "return_all_hits": true,
                "results_verbosity": "compact"
              }
            }""";
}
