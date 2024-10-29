package org.rcsb.stats.tasks;

import org.rcsb.cif.CifIO;
import org.rcsb.cif.ParsingException;
import org.rcsb.cif.model.CifFile;
import org.rcsb.cif.schema.StandardSchemata;
import org.rcsb.stats.Constants;
import org.rcsb.stats.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Obtain the number of non-hydrogen atoms as described by the `atom_site` content of an mmCIF file.
 */
public class Task01_CountHeavyAtoms {
    static final Logger logger = LoggerFactory.getLogger(Task01_CountHeavyAtoms.class);
    final AtomicInteger counter = new AtomicInteger();

    public static void main(String[] args) throws IOException {
        new Task01_CountHeavyAtoms().computeStats();
    }

    void computeStats() throws IOException {
        // obtain set of all known identifiers (1ABC, 1ABD, ...)
        Set<String> identifiers = Helpers.getAllIdentifiers(Constants.ResultsContentType.EXPERIMENTAL);

        // traverse all identifiers
        long heavyAtomCount = identifiers.parallelStream().peek(this::logProgress)
                // load the {@link CifFile} for each identifier
                .map(this::fetchStructureData)
                // process each structure: count the number of non-hydrogen atoms
                .mapToLong(this::countHeavyAtoms)
                // aggregate as sum
                .sum();

        logger.info("There are {} heavy (non-hydrogen) atoms in {} PDB structures", Helpers.formatNumber(heavyAtomCount), Helpers.formatNumber(counter.get()));

        // write results back to table in README.md
        Helpers.updateCount(this.getClass(), heavyAtomCount, counter.get());
    }

    /**
     * Obtain structure for a single identifier from models.rcsb.org.
     * @param identifier datum to load
     * @return structure data in the form of a {@link CifFile}
     */
    CifFile fetchStructureData(String identifier) {
        try {
            URL url = new URL(String.format(Constants.BCIF_SOURCE, identifier));
            // other CifIO methods allow reading from Paths or byte streams -- methods for writing can be found there too
            return CifIO.readFromURL(url);
        } catch (IOException e) {
            logger.warn("Failed to pull structure data for {}", identifier);
            throw new UncheckedIOException(e);
        } catch (ParsingException e) {
            logger.warn("Failed to parse structure data for {}", identifier);
            throw e;
        }
    }

    /**
     * Process a CIF file (obtained from @link{CifIO#readFromUrl()}).
     * @param cifFile source data
     * @return the count of non-hydrogen atoms
     */
    long countHeavyAtoms(CifFile cifFile) {
        return cifFile
                // optional: apply mmCIF schema to get schema definitions and types
                .as(StandardSchemata.MMCIF)
                // CIF files may have multiple blocks of data, the PDB archive only makes use of the 1st
                .getFirstBlock()
                // access the typed `atom_site` category (`.getCategory("atom_site")` would give you a generic category)
                .getAtomSite()
                // access the typed `atom_site.type_symbol` column (`.getColumn("type_symbol")` would give you a generic column)
                .getTypeSymbol()
                // process the element name of all atoms -- typed access provides documentation and types directly from the mmCIF schema
                .values()
                // retain only non-hydrogen atoms
                .filter(this::isHeavyAtom)
                // represent as count of all elements matching the condition
                .count();
    }

    final Set<String> HYDROGEN_ATOMS = Set.of("H", "D", "T");

    /**
     * Filters for non-hydrogen atoms based on their `atom_site.type_symbol`.
     * @param typeSymbol element of this atom
     * @return false if this is hydrogen
     */
    boolean isHeavyAtom(String typeSymbol) {
        return !HYDROGEN_ATOMS.contains(typeSymbol);
    }

    /**
     * Indicate that 10,000 entries have been processed.
     */
    void logProgress(Object ignored) {
        if (counter.incrementAndGet() % 10000 == 0) {
            logger.info("Processed {} entries", Helpers.formatNumber(counter.get()));
        }
    }
}
