package structures;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class ReadPointer implements Iterator<String> {

    private final BufferedReader reader;
    private String currentValue;

    ReadPointer(final BufferedReader reader) throws IOException {
        this.reader = reader;
        currentValue = reader.readLine();
    }

    String getCurrentValue() {
        return currentValue;
    }

    public boolean hasNext() {
        return currentValue != null;
    }

    public String next() {
        if (currentValue == null) {
            return null;
        }
        try {
            currentValue = reader.readLine();
            return currentValue;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    void close() throws IOException {
        reader.close();
    }

    static ReadPointer of(final Path path) throws IOException {
        return new ReadPointer(Files.newBufferedReader(path));
    }
}
