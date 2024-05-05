package structures;

import io.ReadPointer;
import lombok.Getter;
import runner.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * An Attribute resembles a column. It manages its own dependent and referenced attributes.
 */
@Getter
public class Attribute implements Comparable<Attribute> {


    private final int id;
    private final String tableName;
    private final String columnName;
    public int spilledFiles;

    private final PINDList referenced;
    private int dependent;

    private long size;
    private long uniqueSize;
    private long nullCount = 0L;
    private long violationsLeft;

    private ReadPointer readPointer;
    private final Path path;

    private String currentValue;
    private Long currentOccurrences;

    public Attribute(int id, Path attributePath, String tableName, String columnName) {
        this.id = id;
        this.path = attributePath;
        this.tableName = tableName;
        this.columnName = columnName;
        this.referenced = new PINDList();
    }

    public void calculateViolations(Config config) {
        if (config.duplicateHandling == Config.DuplicateHandling.AWARE) {
            this.violationsLeft = (long) ((1.0 - config.threshold) * size);
        } else {
            this.violationsLeft = (long) ((1.0 - config.threshold) * uniqueSize);
        }

        if (config.nullHandling == Config.NullHandling.INEQUALITY) {
            // all nulls are different from each other. Therefore, we need to violate the attribute by the number of nulls
            this.violationsLeft -= this.getNullCount();
        }
    }

    public boolean equals(Attribute other) {
        return currentValue.equals(other.currentValue);
    }

    public int compareBySize(Attribute other) {
        if (this.size > other.getSize()) {
            return -1;
        } else if (this.size == other.size) {
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * Sets the number of dependent ids. Will be used to figure out if the attribute is still a referenced side for other attributes
     *
     * @param dependent number of ids that depend on this attribute
     */
    public void setDependent(int dependent) {
        this.dependent = dependent;
    }

    /**
     * If another attribute does no longer refer this attribute, this method should be called at the instant that this happened
     */
    public void removeDependent() {
        this.dependent--;
    }

    /**
     * Adds all referenced ids from the given Set to the internal referenced set.
     *
     * @param referenced ids of attributes that should be added
     */
    public void addReferenced(List<Integer> referenced) {
        referenced.stream().filter(x -> x != this.getId()).forEach(x -> this.referenced.add(x, violationsLeft));
    }

    /**
     * Updates the internal variables currentValue and currentOccurrences
     *
     * @return True if there was a next value to load, false otherwise
     */
    public boolean nextValue() {
        if (readPointer.hasNext()) {
            this.currentValue = readPointer.next();
            if (readPointer.hasNext()) {
                this.currentOccurrences = Long.valueOf(readPointer.next());
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the dependant and referenced sets using the attributes that share a value.
     *
     * @param attributes     Set of attribute ids, which share some value
     * @param attributeIndex The index that stores all attributes
     */
    public void intersectReferenced(Set<Integer> attributes, final Attribute[] attributeIndex, Config config) {
        PINDList.PINDIterator iterator =  referenced.elementIterator();
        while (iterator.hasNext()) {
            PINDList.PINDElement current = iterator.next();
            int ref = current.id;
            if (attributes.contains(ref)) {
                continue;
            }

            long updated_violations;
            // v won't be null since we iterate the key set
            if (config.duplicateHandling == Config.DuplicateHandling.UNAWARE) {
                updated_violations = current.violate(currentOccurrences);
            } else {
                updated_violations = current.violate(1L);
            }

            if (updated_violations < 0L) {
                iterator.remove();
                attributeIndex[ref].removeDependent();
            }
        }
    }

    /**
     * An Attribute is considered as finished, if it is not referenced by any other and is not dependent on any
     * other attribute. Since it is irrelevant for pIND discovery now, it can be removed from the attribute queue.
     */
    public boolean isNotFinished() {
        return !referenced.isEmpty() || dependent != 0;
    }

    /**
     * Closes the reader connected to the attribute.
     *
     * @throws IOException if the reader fails to close
     */
    public void close() throws IOException {
        readPointer.close();
        Files.delete(readPointer.path);
    }

    public void open() throws IOException {
        this.readPointer = new ReadPointer(path);
        this.currentValue = readPointer.getCurrentValue();
        if (readPointer.hasNext()) this.currentOccurrences = Long.parseLong(readPointer.next());
    }

    public void incNullCount() {
        this.nullCount++;
    }

    @Override
    public int compareTo(Attribute o) {
        if (this.getCurrentValue() == null && o.getCurrentValue() == null) {
            return 0;
        }

        if (this.getCurrentValue() == null) {
            return 1;
        }

        if (o.getCurrentValue() == null) {
            return -1;
        }

        return this.getCurrentValue().compareTo(o.getCurrentValue());
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setUniqueSize(long uniqueSize) {
        this.uniqueSize = uniqueSize;
    }
}
