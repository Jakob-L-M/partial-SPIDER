package io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import runner.Config;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RelationalFileInput implements RelationalInput {

    protected static final String DEFAULT_HEADER_STRING = "column";
    public List<String> headerLine;
    protected CSVReader CSVReader;
    protected List<String> nextLine;
    protected String relationName;
    protected int numberOfColumns = 0;
    // Initialized to -1 because of lookahead
    protected int currentLineNumber = -1;
    protected int numberOfSkippedLines = 0;

    protected boolean hasHeader;
    protected boolean skipDifferingLines;
    protected String nullValue;


    public RelationalFileInput(String relationName, String relationPath, Config config) throws InputIterationException, FileNotFoundException {
        this.relationName = relationName;

        this.hasHeader = config.inputFileHasHeader;
        this.skipDifferingLines = config.inputFileSkipDifferingLines;
        this.nullValue = config.inputFileNullString;

        BufferedReader reader = new BufferedReader(new FileReader(relationPath));

        this.CSVReader = new CSVReaderBuilder(reader).withCSVParser(
                new CSVParserBuilder()
                        .withSeparator(config.separator)
                        .withEscapeChar(config.fileEscape)
                        .withIgnoreLeadingWhiteSpace(config.ignoreLeadingWhiteSpace)
                        .withStrictQuotes(config.strictQuotes)
                        .withQuoteChar(config.quoteChar)
                        .build()
        ).build();

        // read the first line
        this.nextLine = readNextLine();
        if (this.nextLine != null) {
            this.numberOfColumns = this.nextLine.size();
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

    public RelationalFileInput(String relationName, FileReader reader, ConfigurationSettingFileInput setting) throws InputIterationException {
        this.relationName = relationName;

        this.hasHeader = setting.hasHeader();
        this.skipDifferingLines = setting.isSkipDifferingLines();
        this.nullValue = setting.getNullValue();

        this.CSVReader = new CSVReaderBuilder(reader).withCSVParser(new CSVParserBuilder().withSeparator(setting.getSeparatorAsChar()).build()).build();

        // read the first line
        this.nextLine = readNextLine();
        if (this.nextLine != null) {
            this.numberOfColumns = this.nextLine.size();
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

    @Override
    public boolean hasNext() {
        return !(this.nextLine == null);
    }

    @Override
    public List<String> next() throws InputIterationException {
        List<String> currentLine = this.nextLine;

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

    protected void failDifferingLine(List<String> currentLine) throws InputIterationException {
        if (currentLine.size() != this.numberOfColumns()) {
            throw new InputIterationException("Csv line length did not match on line " + currentLineNumber);
        }
    }

    protected void readToNextValidLine() throws InputIterationException {
        if (!hasNext()) {
            return;
        }

        while (this.nextLine.size() != this.numberOfColumns()) {
            this.nextLine = readNextLine();
            this.numberOfSkippedLines++;
            if (!hasNext()) {
                break;
            }
        }
    }

    protected List<String> generateHeaderLine() {
        List<String> headerList = new ArrayList<>();
        for (int i = 1; i <= this.numberOfColumns; i++) {
            headerList.add(DEFAULT_HEADER_STRING + i);
        }
        return Collections.unmodifiableList(headerList);
    }

    protected List<String> readNextLine() throws InputIterationException {
        String[] lineArray;
        try {
            lineArray = this.CSVReader.readNext();
            currentLineNumber++;
        } catch (IOException | CsvValidationException e) {
            throw new InputIterationException("Could not read next line in file input", e);
        }
        if (lineArray == null) {
            return null;
        }
        // Convert empty Strings to null
        List<String> list = new ArrayList<>();
        for (String val : lineArray) {
            if (val.equals(this.nullValue)) {
                list.add(null);
            } else {
                list.add(val);
            }
        }
        // Return an immutable list
        return Collections.unmodifiableList(list);
    }

    @Override
    public void close() throws IOException {
        CSVReader.close();
    }

    @Override
    public int numberOfColumns() {
        return numberOfColumns;
    }

    @Override
    public String relationName() {
        return relationName;
    }

    @Override
    public List<String> columnNames() {
        return headerLine;
    }

}