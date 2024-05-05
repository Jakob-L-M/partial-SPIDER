package io;

import runner.Config;
import structures.Attribute;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public class RepositoryRunner implements Callable<Void> {

    RelationalFileInput table;
    Attribute[] attributeIndex;
    Config config;

    public RepositoryRunner(RelationalFileInput table, Attribute[] attributeIndex, Config config) {
        this.table = table;
        this.attributeIndex = attributeIndex;
        this.config = config;
    }

    @Override
    public Void call() {
        int tableOffset = table.tableOffset;
        Path[] paths = generatePaths(tableOffset, table.numberOfColumns);
        try {
            BufferedWriter[] writers = attachWriters(paths);

            createAttributes(table, attributeIndex, paths);

            store(table, writers, attributeIndex, tableOffset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void createAttributes(RelationalFileInput table, Attribute[] attributeIndex, Path[] paths) {
        for (int i = 0; i < table.numberOfColumns; i++) {
            attributeIndex[table.tableOffset + i] = new Attribute(
                    table.tableOffset + i,
                    paths[i],
                    table.relationName,
                    table.headerLine[i]
            );
        }
    }

    private void store(RelationalFileInput table, BufferedWriter[] writers, Attribute[] attributeIndex, int tableOffset) throws IOException {
        long tableSize = 0L;
        while (table.hasNext()) {
            tableSize++;
            final String[] next = table.next();
            for (int index = 0; index < writers.length; index++) {
                final String value = index >= next.length ? null : next[index];
                if (value != null) {
                    writers[index].write(escape(value));
                    writers[index].newLine();
                } else {
                    attributeIndex[tableOffset + index].incNullCount();
                }
            }
        }
        for (int i = 0; i < writers.length; i++) {
            attributeIndex[tableOffset + i].setSize(tableSize);
            writers[i].flush();
            writers[i].close();
        }
        table.close();
    }

    private BufferedWriter[] attachWriters(Path[] paths) throws IOException {
        BufferedWriter[] writers = new BufferedWriter[paths.length];
        for (int i = 0; i < paths.length; i++) {
            writers[i] = Files.newBufferedWriter(paths[i]);
        }
        return writers;
    }

    private String escape(final String value) {
        return value.replace('\n', '\0');
    }

    private Path[] generatePaths(int tableOffset, int numColumns) {
        Path[] paths = new Path[numColumns];
        for (int i = 0; i < numColumns; i++) {
            File tempFile = new File(config.tempFolder + File.separator  + "a_" + (tableOffset + i) + ".txt");
                    paths[i] = tempFile.toPath();
        }
        return paths;
    }
}
