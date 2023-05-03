package com.protim.batch.config;

import org.springframework.batch.item.ItemProcessor;

import com.protim.batch.entity.Record;

public class RecordProcessor implements ItemProcessor<Record, Record> {

    @Override
    public Record process(Record record) {
        return calculateGrade(record);
    }

    private Record calculateGrade(Record record) {
        int marks = record.getMarks();
        String grade = "A+";
        if (marks < 100) {
            grade = "A";
        }
        if (marks < 90) {
            grade = "B";
        }
        if (marks < 50) {
            grade = "C";
        }
        record.setGrade(grade);

        return record;
    }

}
