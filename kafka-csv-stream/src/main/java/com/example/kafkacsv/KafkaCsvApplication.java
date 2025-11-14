package com.example.kafkacsv;

import com.example.kafkacsv.service.CsvStreamProducer;
import java.nio.file.Path;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaCsvApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaCsvApplication.class, args);
    }

    @Bean
    CommandLineRunner csvIngestionRunner(CsvStreamProducer csvStreamProducer) {
        return args -> {
            Path csvFile = Path.of(System.getProperty("csv.file", "src/main/resources/data/sample-customers.csv"));
            csvStreamProducer.streamCsv(csvFile);
        };
    }
}
