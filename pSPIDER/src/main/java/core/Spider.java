package core;

import io.RelationalFileInput;
import io.RepositoryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.MultiwayMergeSort;
import structures.PINDList;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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


    public void execute() throws IOException, InterruptedException {
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
        priorityQueue = new PriorityQueue<>(numAttributes);

        logger.info("Finished Initializing Attributes. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    /**
     * Iterates over all tables and creates a file for each attribute. Closes the input readers.
     *
     * @param tables Input Files
     */
    private void createAttributes(List<RelationalFileInput> tables) throws InterruptedException {
        logger.info("Creating attribute files");
        long sTime = System.currentTimeMillis();

        ExecutorService executors = Executors.newFixedThreadPool(config.parallel);
        executors.invokeAll(tables.stream().sorted().map(table -> new RepositoryRunner(table, attributeIndex, config)).toList()).forEach(repositoryFuture -> {
            try {
                repositoryFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        executors.shutdown();


        logger.info("Finished creating attribute Files. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    /**
     * Will use the information of the attributeIndex to create a sorted file for each attribute.
     * Attributes are being processed in a LPT fashion for greedy optimality
     *
     * @throws InterruptedException if the executors are interrupted before completion
     */
    private void enqueueAttributes() throws InterruptedException {

        // calculate the available memory based on the parallelization degree
        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long available = memoryUsage.getMax() - memoryUsage.getUsed();
        // we estimate 400 Bytes per String including overhead which is a very generous size
        long threadStringLimit = available / (config.parallel * 400L);
        config.maxMemory = (int) threadStringLimit;

        // create the sort jobs, which are ordered decreasingly by the number of total values
        List<MultiwayMergeSort> sortJobs = Arrays.stream(attributeIndex).sorted(Attribute::compareBySize).map(attribute -> {
            int maxSize = (int) Math.min(attribute.getSize(), config.maxMemory);
            return new MultiwayMergeSort(config, attribute, maxSize);
        }).toList();

        // initialize the executors and process the jobs in parallel
        ExecutorService executors = Executors.newFixedThreadPool(config.parallel);
        executors.invokeAll(sortJobs).forEach(sortFuture -> {
            try {
                sortFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        // release the executors
        executors.shutdown();
    }

    /**
     * This method creates the candidates for the pIND validation. Based on the null handling, the candidates that are generated differ.
     *
     * @throws IOException if an attribute file can not be opened
     */
    private void initializePINDs() throws IOException {
        Arrays.stream(attributeIndex).forEach(attribute -> attribute.calculateViolations(config));
        List<Integer> nonNullIds = Arrays.stream(attributeIndex).filter(attribute -> attribute.getNullCount() == 0).map(Attribute::getId).toList();
        List<Integer> partialNullIds =
                Arrays.stream(attributeIndex).filter(attribute -> attribute.getNullCount() > 0 && attribute.getSize() != attribute.getNullCount()).map(Attribute::getId).toList();
        List<Integer> allNullIds = Arrays.stream(attributeIndex).filter(attribute -> attribute.getNullCount() == attribute.getSize()).map(Attribute::getId).toList();
        for (Attribute attribute : attributeIndex) {

            // Handle Foreign constraints
            if (config.nullHandling == Config.NullHandling.FOREIGN) {
                // initially the attribute references all attributes without nulls
                attribute.addReferenced(nonNullIds);
                // if the attribute itself does not contain nulls it is referenced by every other attributes
                // we can skip the full null attributes, since these will be a subset anyway
                if (attribute.getNullCount() == 0) {
                    attribute.setDependent(attributeIndex.length - allNullIds.size());
                }
            }
            // if the attribute is also an all null attribute
            else if (attribute.getNullCount() == attribute.getSize()) {
                // if we are in inequality mode the attribute can not reference or be referenced by anything

                if (config.nullHandling == Config.NullHandling.EQUALITY) {
                    // since all nulls are equal we know these pINDs will hold
                    attribute.addReferenced(partialNullIds);
                    attribute.addReferenced(allNullIds);
                    // we don't need to set the dependant count size the attribute is considered finished after this step
                } else if (config.nullHandling == Config.NullHandling.SUBSET) {
                    // in subset null is a subset of everything, even null itself
                    attribute.addReferenced(nonNullIds);
                    attribute.addReferenced(partialNullIds);
                    attribute.addReferenced(allNullIds);
                }
            }
            // the attribute is not all null, and we are not in foreign mode
            else {
                attribute.addReferenced(nonNullIds);
                attribute.addReferenced(partialNullIds);
                // initially everything depends on everything excluding all null attributes and itself
                attribute.setDependent(attributeIndex.length - allNullIds.size() - 1);
            }
            // open the file and add it to the priority queue if the attribute contains values
            attribute.open();
            if (attribute.getReadPointer().hasNext()) {
                priorityQueue.add(attribute);
            }
        }

    }

    private int getTotalColumnCount(final List<RelationalFileInput> tables) {
        return tables.stream().mapToInt(RelationalFileInput::numberOfColumns).sum();
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
                while (firstAttribute.nextValue() && firstAttribute.isNotFinished()) {
                    if (firstAttribute.getCurrentValue().compareTo(nextVal) >= 0) {
                        priorityQueue.add(firstAttribute);
                        break;
                    }
                }
            } else {

                for (int topAttribute : topAttributes.keySet()) {
                    final Attribute attribute = attributeIndex[topAttribute];
                    if (attribute.nextValue() && attribute.isNotFinished()) {
                        priorityQueue.add(attribute);
                    }
                }
            }

            topAttributes.clear();
        }
        logger.info("Finished pIND calculation. Took: " + (System.currentTimeMillis() - sTime) + "ms");
    }

    private void collectResults(long init, long enqueue, long pINDCreation, long pINDValidation) throws IOException {
        int numUnary = Arrays.stream(attributeIndex).mapToInt(attribute -> attribute.getReferenced().size()).sum();
        logger.info("Found " + numUnary + " pINDs");
        BufferedWriter bw;
        if (config.writeResults) {
            bw = new BufferedWriter(new FileWriter(".\\results\\" + config.executionName + "_pINDs.txt"));

            for (final Attribute dep : attributeIndex) {

                if (dep.getReferenced().isEmpty()) {
                    continue;
                }

                PINDList.PINDIterator iterator = dep.getReferenced().elementIterator();
                while (iterator.hasNext()) {
                    final Attribute ref = attributeIndex[iterator.next().id];

                    bw.write(dep.getTableName() + " " + dep.getColumnName());
                    bw.write(" < ");
                    bw.write(ref.getTableName() + " " + ref.getColumnName());
                    bw.write('\n');

                }
            }
            bw.close();
        }

        bw = new BufferedWriter(new FileWriter(".\\results\\" + config.executionName + "_" + System.currentTimeMillis() + ".json"));
        // build a json file
        bw.write('{');
        bw.write("\"database\": \"" + config.databaseName + "\",");
        bw.write("\"threads\": " + config.parallel + ",");
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
        bw.close();
    }

    private void shutdown() throws IOException {
        for (final Attribute attribute : attributeIndex) {
            attribute.close();
        }

    }
}
