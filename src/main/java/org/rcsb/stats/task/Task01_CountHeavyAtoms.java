package org.rcsb.stats.task;

import org.rcsb.cif.schema.mm.MmCifFile;
import org.rcsb.stats.Helpers;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Task01_CountHeavyAtoms {
    public static void main(String[] args) throws IOException {
        Set<String> identifiers = Helpers.getAllIdentifiers();

        AtomicInteger counter = new AtomicInteger();
        long heavyAtomCount = Helpers.fetchStructureData(identifiers)
                .peek(i -> { if (counter.incrementAndGet() % 10000 == 0) System.out.println("Processed " + Helpers.formatNumber(counter.get()) + " entries"); })
                .mapToLong(Task01_CountHeavyAtoms::countHeavyAtoms)
                .sum();

        System.out.println("There are " + Helpers.formatNumber(heavyAtomCount) + " heavy (non-hydrogen) atoms in " + Helpers.formatNumber(counter.get()) + " PDB structures");
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
