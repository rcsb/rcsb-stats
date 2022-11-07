package org.rcsb.stats.task;

import org.rcsb.cif.CifIO;
import org.rcsb.cif.schema.StandardSchemata;
import org.rcsb.cif.schema.mm.MmCifFile;
import org.rcsb.stats.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Task01_CountHeavyAtoms {
    private static final Logger logger = LoggerFactory.getLogger(Task01_CountHeavyAtoms.class);

    public static void main(String[] args) throws IOException {
        AtomicInteger counter = new AtomicInteger();
        long heavyAtomCount = Files.list(Paths.get(Constants.BCIF_DIR))
                .parallel()
                .peek(i -> { if (counter.incrementAndGet() % 10000 == 0) logger.info("Processed {} entries", counter.get()); })
                .mapToLong(Task01_CountHeavyAtoms::countHeavyAtoms)
                .sum();

        logger.info("There are {} heavy (non-hydrogen) atoms in {} PDB structures", heavyAtomCount, counter.get());
    }

    private static long countHeavyAtoms(Path path) {
        try {
            MmCifFile cifFile = CifIO.readFromPath(path).as(StandardSchemata.MMCIF);
            return cifFile.getFirstBlock()
                    .getAtomSite()
                    .getTypeSymbol()
                    .values()
                    .filter(Task01_CountHeavyAtoms::isHeavyAtom)
                    .count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final Set<String> HYDROGEN_ATOMS = Set.of("H", "D", "T");
    private static boolean isHeavyAtom(String typeSymbol) {
        return !HYDROGEN_ATOMS.contains(typeSymbol);
    }
}
