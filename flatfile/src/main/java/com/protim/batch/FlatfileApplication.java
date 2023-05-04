package com.protim.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class FlatfileApplication {

	public static void main(String[] args) {
		SpringApplication.run(FlatfileApplication.class, args);

	}

}
