package com.protim.batch.entity;

public class Record {

    int id;
    String name;
    String subject;
    int marks;
    String grade;
    String spec;

    @Override
    public String toString() {
        return "Record [id=" + id + ", name=" + name + ", subject=" + subject + ", marks=" + marks + ", grade=" + grade
                + ", spec=" + spec + "]";
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getMarks() {
        return marks;
    }

    public void setMarks(int marks) {
        this.marks = marks;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }

    public String getSpec() {
        return spec;
    }

    public void setSpec(String spec) {
        this.spec = spec;
    }

    public String getCsv() {
        return id + "," + "\"" + name + "\"" + "," + "\"" + subject + "\"" + "," + "\"" + grade + "\"" + ","
                + marks
                + "," + "\"" + spec + "\"";
    }

}
