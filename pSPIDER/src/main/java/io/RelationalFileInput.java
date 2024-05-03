package io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import runner.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class RelationalFileInput {

    protected static final String DEFAULT_HEADER_STRING = "column";
    public String[] headerLine;
    public int tableOffset;
    protected CSVReader CSVReader;
    protected String[] nextLine;
    protected String relationName;
    protected int numberOfColumns = 0;
    // Initialized to -1 because of lookahead
    protected int currentLineNumber = -1;
    protected int numberOfSkippedLines = 0;

    protected boolean hasHeader;
    protected boolean skipDifferingLines;
    protected String nullValue;
    private final Config.NullHandling nullHandling;


    public RelationalFileInput(String relationName, String relationPath, Config config, int tableOffset) throws IOException {
        this.relationName = relationName;
        this.tableOffset = tableOffset;

        this.hasHeader = config.header;
        this.skipDifferingLines = config.skipDifferingLines;
        this.nullValue = config.nullString;

        this.nullHandling = config.nullHandling;

        BufferedReader reader = new BufferedReader(new FileReader(relationPath));

        this.CSVReader = new CSVReaderBuilder(reader).withCSVParser(new CSVParserBuilder().withSeparator(config.separator).withEscapeChar(config.fileEscape).withIgnoreLeadingWhiteSpace(config.ignoreLeadingWhiteSpace).withStrictQuotes(config.strictQuotes).withQuoteChar(config.quoteChar).build()).build();

        // read the first line
        this.nextLine = readNextLine();
        if (this.nextLine != null) {
            this.numberOfColumns = this.nextLine.length;
        }

        if (hasHeader) {
            this.headerLine = this.nextLine;
            next();
        }

        // If the header is still null generate a standard header the size of number of columns.
        if (this.headerLine == null) {
            this.headerLine = generateHeaderLine();
        }
    }

    public boolean hasNext() {
        return !(this.nextLine == null);
    }

    public String[] next() throws IOException {
        String[] currentLine = this.nextLine;

        if (currentLine == null) {
            return null;
        }
        this.nextLine = readNextLine();

        if (this.skipDifferingLines) {
            readToNextValidLine();
        } else {
            failDifferingLine(currentLine);
        }

        return currentLine;
    }

    protected void failDifferingLine(String[] currentLine) throws IOException {
        if (currentLine.length != this.numberOfColumns()) {
            throw new IOException("");
        }
    }

    protected void readToNextValidLine() {
        if (!hasNext()) {
            return;
        }

        while (this.nextLine.length != this.numberOfColumns()) {
            this.nextLine = readNextLine();
            this.numberOfSkippedLines++;
            if (!hasNext()) {
                break;
            }
        }
    }

    protected String[] generateHeaderLine() {
        String[] headerList = new String[numberOfColumns];
        for (int i = 1; i <= this.numberOfColumns; i++) {
            headerList[i-1] = DEFAULT_HEADER_STRING + i;
        }
        return headerList;
    }

    protected String[] readNextLine() {
        String[] lineArray = null;
        try {
            lineArray = this.CSVReader.readNext();
            currentLineNumber++;
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }
        if (lineArray == null) {
            return null;
        }
        // Convert empty Strings to null
        if (nullHandling != Config.NullHandling.EQUALITY) {
            for (int i = 0; i < lineArray.length; i++) {
                if (lineArray[i].equals(this.nullValue)) {
                    lineArray[i] = null;
                }
            }
        }
        return lineArray;
    }

    public void close() throws IOException {
        CSVReader.close();
    }

    public int numberOfColumns() {
        return numberOfColumns;
    }

}