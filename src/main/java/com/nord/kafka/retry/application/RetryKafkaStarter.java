package com.nord.kafka.retry.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RetryKafkaStarter {

	public static void main(String[] args) {
		SpringApplication.run(RetryKafkaStarter.class, args);
	}

}
