package structures;

import io.Merger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Callable;

public class MultiwayMergeSort implements Callable<Void> {

    private final Path origin;
    private final int mapLimit;
    private final Logger logger;
    private final Attribute attribute;
    private final int maxMapSize;
    private Map<String, Long> values;
    private List<Path> spilledFiles;
    private int valuesSinceLastSpill;

    public MultiwayMergeSort(Config config, Attribute attribute, int stringLimit) {
        this.attribute = attribute;
        this.mapLimit = stringLimit;
        this.valuesSinceLastSpill = 0;
        this.origin = attribute.getPath();
        this.logger = LoggerFactory.getLogger(MultiwayMergeSort.class);
        this.maxMapSize = config.maxMemory;
    }

    @Override
    public Void call() throws IOException {
        logger.debug("Starting sort for: " + attribute.getId());
        this.values = new HashMap<>(mapLimit / 4);
        this.spilledFiles = new ArrayList<>();

        long sTime = System.currentTimeMillis();

        // one file is created when merging
        attribute.spilledFiles = 1;

        this.writeSpillFiles();
        if (this.spilledFiles.isEmpty()) {
            attribute.setUniqueSize(this.values.size());
            this.write(origin);
        } else {
            if (!this.values.isEmpty()) {
                this.writeSpillFile();
            }
            Merger spilledMerger = new Merger();
            spilledMerger.merge(this.spilledFiles, this.origin, attribute);
        }

        attribute.spilledFiles += spilledFiles.size();
        this.removeSpillFiles();

        // release memory
        values = null;

        logger.debug("Finished sort for: " + attribute.getId() + ". Took: " + (System.currentTimeMillis() - sTime));
        return null;
    }

    private void writeSpillFiles() throws IOException {
        BufferedReader reader = Files.newBufferedReader(this.origin);

        String line;
        while ((line = reader.readLine()) != null) {
            if (1L == this.values.compute(line, (k, v) -> v == null ? 1L : v + 1L)) {
                this.maybeWriteSpillFile();
            }
        }

        reader.close();
    }


    private void maybeWriteSpillFile() throws IOException {
        ++this.valuesSinceLastSpill;
        if (this.valuesSinceLastSpill > this.maxMapSize) {
            this.valuesSinceLastSpill = 0;
            this.writeSpillFile();
        }

    }

    private void writeSpillFile() throws IOException {
        logger.debug("Spilling Attribute " + this.origin + "#" + this.spilledFiles.size());
        Path target = Paths.get(this.origin + "#" + this.spilledFiles.size());
        this.write(target);
        this.spilledFiles.add(target);
        this.values.clear();
    }

    private void write(Path path) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        values.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            try {
                writer.write(entry.getKey());
                writer.newLine();
                writer.write(entry.getValue().toString());
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        writer.flush();
        writer.close();
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

        spilledFiles = null;
    }

}
