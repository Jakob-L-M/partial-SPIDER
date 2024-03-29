package structures;

import io.Merger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class MultiwayMergeSort {

    private final Path origin;
    private final long maxMemoryUsage;
    private final Map<String, Long> values;
    private final List<Path> spilledFiles;
    private final int memoryCheckFrequency;
    private final Logger logger;
    private final Attribute attribute;
    private int valuesSinceLastMemoryCheck;

    public MultiwayMergeSort(Config config, Attribute attribute) {
        this.values = new TreeMap<>();
        this.attribute = attribute;
        this.spilledFiles = new ArrayList<>();
        this.valuesSinceLastMemoryCheck = 0;
        this.origin = attribute.getPath();
        this.memoryCheckFrequency = config.memoryCheckFrequency;
        this.maxMemoryUsage = getMaxMemoryUsage(config.maxMemoryPercent);
        this.logger = LoggerFactory.getLogger(MultiwayMergeSort.class);
    }

    private static long getMaxMemoryUsage(int maxMemoryPercent) {
        long available = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        return (long) ((double) available * ((double) maxMemoryPercent / 100.0D));
    }

    public void sort() throws IOException {
        logger.info("Starting sort for: " + attribute.getId());
        long sTime = System.currentTimeMillis();

        this.writeSpillFiles();
        if (this.spilledFiles.isEmpty()) {
            attribute.setUniqueSize(this.values.size());
            this.writeOutput();
        } else {
            if (!this.values.isEmpty()) {
                this.writeSpillFile();
            }
            Merger spilledMerger = new Merger();
            spilledMerger.merge(this.spilledFiles, this.origin, attribute);
        }

        attribute.spilledFiles = spilledFiles.size();
        this.removeSpillFiles();

        logger.info("Finished sort for: " + attribute.getId() + ". Took: " + (System.currentTimeMillis() - sTime));
    }

    private void writeSpillFiles() throws IOException {
        BufferedReader reader = Files.newBufferedReader(this.origin);

        String line;
        while ((line = reader.readLine()) != null) {
            if (1L == this.values.compute(line, (k, v) -> v == null ? 1L : v+1L)) {
                this.maybeWriteSpillFile();
            }
        }

        reader.close();
    }


    private void maybeWriteSpillFile() throws IOException {
        ++this.valuesSinceLastMemoryCheck;
        if (this.valuesSinceLastMemoryCheck > this.memoryCheckFrequency && this.shouldWriteSpillFile()) {
            this.valuesSinceLastMemoryCheck = 0;
            this.writeSpillFile();
        }

    }

    private boolean shouldWriteSpillFile() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > this.maxMemoryUsage;
    }

    private void writeSpillFile() throws IOException {
        logger.info("Spilling Attribute " + this.origin + "#" + this.spilledFiles.size());
        Path target = Paths.get(this.origin + "#" + this.spilledFiles.size());
        this.write(target, this.values);
        this.spilledFiles.add(target);
        this.values.clear();
        System.gc();
    }

    private void write(Path path, Map<String, Long> values) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE);

        Iterator<Long> valueIterator = values.values().iterator();
        for (String key : values.keySet()) {
            writer.write(key);
            writer.newLine();
            writer.write(valueIterator.next().toString());
            writer.newLine();
        }
        writer.flush();
        writer.close();
    }

    private void writeOutput() throws IOException {
        BufferedWriter output = Files.newBufferedWriter(this.origin);

        Iterator<Long> valueIterator = this.values.values().iterator();

        for (String key : values.keySet()) {
            output.write(key);
            output.newLine();
            output.write(valueIterator.next().toString());
            output.newLine();
        }
        output.flush();
        output.close();
    }

    private void removeSpillFiles() {
        spilledFiles.forEach(x -> {
            try {
                Files.delete(x);
            } catch (IOException e) {
                System.out.println("Unable to delete file: " + x);
                e.printStackTrace();
            }
        });

        this.spilledFiles.clear();
    }
}
