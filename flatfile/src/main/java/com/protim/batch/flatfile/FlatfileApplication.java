package com.protim.batch.flatfile;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlatfileApplication {

	private static String[] names = { "Amit", "John", "David", "Mary", "Carlos", "Subir", "Chris" };
	private static String[] subjects = { "Physics", "Biology", "Mathematics", "History", "Arts" };
	private static int[] marks = { 100, 98, 65, 44 };
	private static String[] specs = { "major", "minor" };

	private static final String FILE = "input.csv";

	public static void main(String[] args) {
		SpringApplication.run(FlatfileApplication.class, args);

	}

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

}
