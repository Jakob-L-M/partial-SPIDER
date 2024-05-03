package core;

import io.RelationalFileInput;
import io.RepositoryRunner;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.MultiwayMergeSort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.stream.Collectors;

public class Spider {

    private final Config config;
    private final Logger logger;
    private final long startTime;
    private Attribute[] attributeIndex;
    private PriorityQueue<Attribute> priorityQueue;


    public Spider(Config config) {
        this.config = config;
        this.logger = LoggerFactory.getLogger(Spider.class);
        this.startTime = System.currentTimeMillis();
    }


    public void execute() throws IOException {
        logger.info("Starting Execution");
        List<RelationalFileInput> tables = this.config.getFileInputs();

        initializeAttributes(tables);

        long initializingTime = System.currentTimeMillis();
        createAttributes(tables);
        initializingTime = System.currentTimeMillis() - initializingTime;

        long enqueueTime = System.currentTimeMillis();
        enqueueAttributes();
        enqueueTime = System.currentTimeMillis() - enqueueTime;

        long pINDInitialization = System.currentTimeMillis();
        initializePINDs();
        pINDInitialization = System.currentTimeMillis() - pINDInitialization;

        long pINDCalculation = System.currentTimeMillis();
        calculateInclusionDependencies();
        pINDCalculation = System.currentTimeMillis() - pINDCalculation;

        collectResults(initializingTime, enqueueTime, pINDInitialization, pINDCalculation);
        shutdown();
    }

    /**
     * Fetches the number of attributes and prepares the index as well as the priority queue
     *
     * @param tables Input Files
     */
    private void initializeAttributes(final List<RelationalFileInput> tables) {
        logger.info("Initializing Attributes");
        long sTime = System.currentTimeMillis();

        int numAttributes = getTotalColumnCount(tables);
        logger.info("Found " + numAttributes + " attributes");
        attributeIndex = new Attribute[numAttributes];
        priorityQueue = new PriorityQueue<>(numAttributes, this::compareAttributes);

        logger.info("Finished Initializing Attributes. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    /**
     * Iterates over all tables and creates a file for each attribute. Closes the input readers.
     *
     * @param tables Input Files
     */
    private void createAttributes(List<RelationalFileInput> tables) {
        logger.info("Creating attribute files");
        long sTime = System.currentTimeMillis();

        tables.parallelStream().forEach(table -> {
            RepositoryRunner repositoryRunner = new RepositoryRunner(table, attributeIndex, config);
            repositoryRunner.run();
        });

        logger.info("Finished creating attribute Files. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private void enqueueAttributes() throws IOException {

        Queue<Attribute> attributeQueue = Arrays.stream(attributeIndex).sorted(Attribute::compareBySize).collect(Collectors.toCollection(ArrayDeque::new));
        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long available = memoryUsage.getMax() - memoryUsage.getUsed();
        // we estimate 400 Bytes per String including overhead
        long threadStringLimit = available / (config.numThreads*400L);
        config.maxMemory = (int) threadStringLimit;

        attributeQueue.parallelStream().forEach(attribute -> {
            int maxSize = (int) Math.min(attribute.getSize(), config.maxMemory);
            MultiwayMergeSort multiwayMergeSort = new MultiwayMergeSort(config, attribute, maxSize);
            try {
                multiwayMergeSort.sort();
            } catch (IOException e) {
                e.printStackTrace();
            }
            attribute.calculateViolations(config);
        });

        for (final Attribute attribute : attributeIndex) {
            attribute.open();
            if (attribute.getReadPointer().hasNext()) {
                priorityQueue.add(attribute);
            } else {
                // The attribute is null in every entry
                // Equality will never get here, since null is considered a value
                if (config.nullHandling == Config.NullHandling.INEQUALITY) {
                    // Inequality: Every Null is different form every other null
                    // An attribute that only consists of null can not form any pIND regardless of the threshold.
                    attribute.getDependent().clear();
                    attribute.getReferenced().clear();
                } else {
                    // Subset: Attribute references everything
                    // Another Pure-Null attribute could still be a reference

                    // Foreign: Like Subset but referenced can not include null -> handled below
                    attribute.getDependent().clear();
                }
            }
        }


        // Handle Foreign constraints
        if (config.nullHandling == Config.NullHandling.FOREIGN) {
            for (Attribute attribute : attributeIndex) {
                if (attribute.getNullCount() > 0L) {
                    for (Attribute depAttribute : attributeIndex) {
                        depAttribute.removeReferenced(attribute.getId());
                    }
                }
            }
        }
    }

    private void initializePINDs() {
        final IntSet allIds = allIds();
        for (final Attribute attribute : attributeIndex) {
            attribute.addDependent(allIds);
            attribute.removeDependent(attribute.getId());
            attribute.addReferenced(allIds);
            attribute.removeReferenced(attribute.getId());
        }
    }

    private IntSet allIds() {
        final IntSet ids = new IntOpenHashSet(attributeIndex.length);
        for (int index = 0; index < attributeIndex.length; ++index) {
            ids.add(index);
        }
        return ids;
    }

    private int getTotalColumnCount(final List<RelationalFileInput> tables) {
        return tables.stream().mapToInt(RelationalFileInput::numberOfColumns).sum();
    }

    private int compareAttributes(final Attribute a1, final Attribute a2) {
        if (a1.getCurrentValue() == null && a2.getCurrentValue() == null) {
            return 0;
        }

        if (a1.getCurrentValue() == null) {
            return 1;
        }

        if (a2.getCurrentValue() == null) {
            return -1;
        }

        return a1.getCurrentValue().compareTo(a2.getCurrentValue());
    }

    private void calculateInclusionDependencies() {
        long sTime = System.currentTimeMillis();
        logger.info("Start pIND calculation");

        Map<Integer, Long> topAttributes = new HashMap<>();
        while (!priorityQueue.isEmpty()) {

            final Attribute firstAttribute = priorityQueue.poll();
            topAttributes.put(firstAttribute.getId(), firstAttribute.getCurrentOccurrences());
            while (!priorityQueue.isEmpty() && priorityQueue.peek().equals(firstAttribute)) {
                Attribute sameGroupAttribute = priorityQueue.poll();
                topAttributes.put(sameGroupAttribute.getId(), sameGroupAttribute.getCurrentOccurrences());
            }

            for (int topAttribute : topAttributes.keySet()) {
                attributeIndex[topAttribute].intersectReferenced(topAttributes.keySet(), attributeIndex, config);
            }

            if (topAttributes.size() == 1 && !priorityQueue.isEmpty()) {
                String nextVal = priorityQueue.peek().getCurrentValue();
                while (firstAttribute.nextValue() && !firstAttribute.isFinished()) {
                    if (firstAttribute.getCurrentValue().compareTo(nextVal) >= 0) {
                        priorityQueue.add(firstAttribute);
                        break;
                    }
                }
            } else {

                for (int topAttribute : topAttributes.keySet()) {
                    final Attribute attribute = attributeIndex[topAttribute];
                    if (attribute.nextValue() && !attribute.isFinished()) {
                        priorityQueue.add(attribute);
                    }
                }
            }

            topAttributes.clear();
        }
        logger.info("Finished pIND calculation. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private void collectResults(long init, long enqueue, long pINDCreation, long pINDValidation) throws IOException {
        int numUnary = 0;
        BufferedWriter bw = new BufferedWriter(new FileWriter(".\\results\\" + config.executionName + "_pINDs.txt"));

        for (final Attribute dep : attributeIndex) {

            if (dep.getReferenced().isEmpty()) {
                continue;
            }

            for (final int refId : dep.getReferenced().keySet()) {
                numUnary++;
                final Attribute ref = attributeIndex[refId];

                bw.write(dep.getTableName() + " " + dep.getColumnName());
                bw.write(" < ");
                bw.write(ref.getTableName() + " " + ref.getColumnName());
                bw.write('\n');

            }
        }
        bw.flush();
        bw.close();

        bw = new BufferedWriter(new FileWriter(".\\results\\" + config.executionName + "_" + (System.currentTimeMillis()/1000) + ".json"));
        // build a json file
        bw.write('{');
        bw.write("\"database\": \"" + config.databaseName + "\",");
        bw.write("\"threads\": " + config.numThreads + ",");
        bw.write("\"pINDs\": " + numUnary + ",");
        bw.write("\"threshold\": " + config.threshold + ",");
        bw.write("\"nullHandling\": \"" + config.nullHandling + "\",");
        bw.write("\"duplicateHandling\": \"" + config.duplicateHandling + "\",");
        bw.write("\"initialization\": " + init + ",");
        bw.write("\"enqueue\": " + enqueue + ",");
        bw.write("\"pINDCreation\": " + pINDCreation + ",");
        bw.write("\"pINDValidation\": " + pINDValidation + ",");
        bw.write("\"total_time\": " + (System.currentTimeMillis() - startTime) + ",");
        // the total of spilled files + the copied attribute file
        bw.write("\"spilledFiles\": " + (Arrays.stream(attributeIndex).mapToInt(Attribute::getSpilledFiles).sum() + attributeIndex.length));
        bw.write('}');
        bw.flush();
        bw.close();
    }

    private void shutdown() throws IOException {
        for (final Attribute attribute : attributeIndex) {
            attribute.close();
        }

    }
}
