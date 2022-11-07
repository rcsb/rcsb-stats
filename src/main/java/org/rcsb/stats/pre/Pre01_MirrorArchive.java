package org.rcsb.stats.pre;

import org.rcsb.stats.Constants;
import org.rcsb.stats.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.rcsb.stats.Helpers.getAllIdentifiers;

public class Pre01_MirrorArchive {
    private static final Logger logger = LoggerFactory.getLogger(Pre01_MirrorArchive.class);

    public static void main(String[] args) throws IOException {
        Path dir = Paths.get(Constants.BCIF_DIR);
        logger.info("Clearing out dir at {}", dir);
        Helpers.deleteDirectory(dir);
        logger.info("Ensuring {} exists", dir);
        Helpers.createDirectory(dir);

        AtomicInteger counter = new AtomicInteger();
        getAllIdentifiers()
                .parallelStream()
                .peek(i -> { if (counter.incrementAndGet() % 10000 == 0) logger.info("Processed {} entries", counter.get()); })
                .forEach(i -> {
                    try {
                        Helpers.downloadFromUrl(new URL(String.format(Constants.BCIF_SOURCE, i)), dir.resolve(i + ".bcif.gz"));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        logger.info("Downloaded {} files", Files.list(dir).count());
    }
}
