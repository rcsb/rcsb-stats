package org.rcsb.stats.tasks;

import org.rcsb.cif.schema.mm.MmCifFile;
import org.rcsb.stats.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Task01_CountHeavyAtoms {
    private static final Logger logger = LoggerFactory.getLogger(Task01_CountHeavyAtoms.class);

    public static void main(String[] args) throws IOException {
        new Task01_CountHeavyAtoms().computeStats();
    }

    void computeStats() throws IOException {
        Set<String> identifiers = Helpers.getAllIdentifiers();

        AtomicInteger counter = new AtomicInteger();
        long heavyAtomCount = Helpers.fetchStructureData(identifiers)
                .limit(1000)
                .peek(i -> { if (counter.incrementAndGet() % 10000 == 0) logger.info("Processed {} entries", Helpers.formatNumber(counter.get())); })
                .mapToLong(Task01_CountHeavyAtoms::countHeavyAtoms)
                .sum();
        logger.info("There are {} heavy (non-hydrogen) atoms in {} PDB structures", heavyAtomCount, Helpers.formatNumber(counter.get()));

        Helpers.updateCount(this.getClass(), heavyAtomCount, counter.get());
    }

    private static long countHeavyAtoms(MmCifFile cifFile) {
        return cifFile.getFirstBlock()
                .getAtomSite()
                .getTypeSymbol()
                .values()
                .filter(Task01_CountHeavyAtoms::isHeavyAtom)
                .count();
    }

    private static final Set<String> HYDROGEN_ATOMS = Set.of("H", "D", "T");
    private static boolean isHeavyAtom(String typeSymbol) {
        return !HYDROGEN_ATOMS.contains(typeSymbol);
    }
}
