package com.protim.batch.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;

import com.protim.batch.entity.Record;

/**
 * Utility Class for CSV Batch
 */
public class Helper {

    private static String[] names = { "Amit", "John", "David", "Mary", "Carlos", "Subir", "Chris" };
    private static String[] subjects = { "Physics", "Biology", "Mathematics", "History", "Arts" };
    private static int[] marks = { 100, 98, 65, 44 };
    private static String[] specs = { "major", "minor" };

    private static final String FILE = "input.csv";

    /**
     * Creates CSV File for the batch to process
     */
    public static void createCsv() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE));) {
            String header = "\"Id\",\"Name\",\"Subject\",\"Marks\",\"Specs\"\n";
            writer.write(header);
            for (int i = 0; i < 50000; i++) {
                int name = (int) (Math.random() * (6 - 0 + 1) + 0);
                int subject = (int) (Math.random() * (4 - 0 + 1) + 0);
                int mark = (int) (Math.random() * (3 - 0 + 1) + 0);
                int spec = (int) (Math.random() * (1 - 0 + 1) + 0);
                String line = (i + 1) + "," + "\"" + names[name] + "\"" + "," + "\"" + subjects[subject] + "\"" + ","
                        + marks[mark]
                        + "," + "\"" + specs[spec] + "\"";
                System.out.println("Line[" + i + "]: " + line);
                writer.write(line + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FlatFileItemReader<Record> readerWithoutAnClassExample() {
        FlatFileItemReader<Record> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("flatfile/src/main/resources/input.csv"));
        itemReader.setName("csvFileReader");
        itemReader.setLinesToSkip(1);

        // Create Line Mapper for the reader
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("id", "name", "subject", "marks", "spec");
        lineTokenizer.setStrict(false);
        lineTokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_COMMA);

        BeanWrapperFieldSetMapper<Record> fieldSetMapper = new BeanWrapperFieldSetMapper<Record>();
        fieldSetMapper.setTargetType(Record.class);

        DefaultLineMapper<Record> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        itemReader.setLineMapper(lineMapper);
        return itemReader;
    }

}
