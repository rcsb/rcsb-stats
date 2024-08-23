package org.rcsb.stats.tasks;

import org.rcsb.cif.model.CifFile;
import org.rcsb.cif.schema.StandardSchemata;
import org.rcsb.stats.Constants;
import org.rcsb.stats.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Obtain the number of non-hydrogen atoms as described by the `atom_site` content of a mmCIF file.
 */
public class Task01_CountHeavyAtoms {
    private static final Logger logger = LoggerFactory.getLogger(Task01_CountHeavyAtoms.class);

    public static void main(String[] args) throws IOException {
        new Task01_CountHeavyAtoms().computeStats();
    }

    void computeStats() throws IOException {
        // request set of all known identifiers
        Set<String> identifiers = Helpers.getAllIdentifiers(Set.of(Constants.ResultsContentType.EXPERIMENTAL));
        AtomicInteger counter = new AtomicInteger();

        // obtain stream of CifFiles
        long heavyAtomCount = Helpers.fetchStructureData(identifiers)
                // log progress every 10,000 elements
                .peek(i -> { if (counter.incrementAndGet() % 10000 == 0) logger.info("Processed {} entries", Helpers.formatNumber(counter.get())); })
                // transform structure into number of atoms
                .mapToLong(this::countHeavyAtoms)
                // aggregate as sum
                .sum();

        logger.info("There are {} heavy (non-hydrogen) atoms in {} PDB structures", Helpers.formatNumber(heavyAtomCount), Helpers.formatNumber(counter.get()));

        // write results back to table in README.md
        Helpers.updateCount(this.getClass(), heavyAtomCount, counter.get());
    }

    /**
     * Process a CIF file.
     * @param cifFile source data
     * @return the count of non-hydrogen atoms
     */
    long countHeavyAtoms(CifFile cifFile) {
        return cifFile
                // optionally, apply mmCIF schema to get schema definitions and types
                .as(StandardSchemata.MMCIF)
                // CIF files may have multiple blocks of data, the PDB archive only makes use of the 1st
                .getFirstBlock()
                // access `atom_site` category
                .getAtomSite()
                // access `atom_site.type_symbol` column
                .getTypeSymbol()
                // stream over all element names of all atoms
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
}
