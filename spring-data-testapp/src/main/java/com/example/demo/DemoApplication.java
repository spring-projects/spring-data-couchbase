package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class DemoApplication {

	char a = '\u00A0';
	public static void main(String[] args) {
		new SpringApplicationBuilder ( DemoApplication.class)
				.properties("spring.main.allow-bean-definition-overriding=true")
				.build()
				.run(args);
	}


}
