package com.example.kafkacsv.service;

import com.example.kafkacsv.model.CustomerRecord;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

@Service
public class CsvStreamProducer {

    private static final Logger log = LoggerFactory.getLogger(CsvStreamProducer.class);
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final KafkaTemplate<String, CustomerRecord> kafkaTemplate;
    private final String topicName;

    public CsvStreamProducer(
            KafkaTemplate<String, CustomerRecord> kafkaTemplate,
            @Value("${app.kafka.customer-topic:customers.csv.ingested}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void streamCsv(Path csvFile) throws IOException {
        log.info("Starting ingestion of CSV file: {}", csvFile.toAbsolutePath());
        StopWatch watch = new StopWatch();
        watch.start();
        AtomicLong successCounter = new AtomicLong();
        AtomicLong failureCounter = new AtomicLong();

        try (Reader reader = Files.newBufferedReader(csvFile);
                CSVParser parser = CSVFormat.DEFAULT
                        .builder()
                        .setHeader("id", "first_name", "last_name", "email", "signup_date", "loyalty_points")
                        .setSkipHeaderRecord(true)
                        .build()
                        .parse(reader)) {
            parser.forEach(record -> processRecord(record, successCounter, failureCounter));
        }

        watch.stop();
        log.info(
                "Finished ingestion. success={}, failed={}, elapsed={} ms",
                successCounter.get(),
                failureCounter.get(),
                watch.getTotalTimeMillis());
    }

    private void processRecord(CSVRecord csvRecord, AtomicLong successCounter, AtomicLong failureCounter) {
        try {
            CustomerRecord payload = mapRecord(csvRecord);
            kafkaTemplate.send(topicName, payload.id(), payload);
            successCounter.incrementAndGet();
        } catch (IllegalArgumentException ex) {
            failureCounter.incrementAndGet();
            log.warn("Invalid record on line {}: {}", csvRecord.getRecordNumber(), ex.getMessage());
        }
    }

    private CustomerRecord mapRecord(CSVRecord csvRecord) {
        String id = require(csvRecord, "id");
        String email = require(csvRecord, "email");
        if (!EMAIL_PATTERN.matcher(email).matches()) {
            throw new IllegalArgumentException("email invalide: " + email);
        }

        String loyaltyPointsValue = require(csvRecord, "loyalty_points");
        int loyaltyPoints;
        try {
            loyaltyPoints = Integer.parseInt(loyaltyPointsValue);
            if (loyaltyPoints < 0) {
                throw new IllegalArgumentException("loyalty_points doit etre positif");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("loyalty_points n'est pas un entier");
        }

        LocalDate signupDate;
        try {
            signupDate = LocalDate.parse(require(csvRecord, "signup_date"), DATE_FORMATTER);
        } catch (Exception e) {
            throw new IllegalArgumentException("signup_date doit suivre le format ISO (yyyy-MM-dd)");
        }

        return new CustomerRecord(
                id,
                require(csvRecord, "first_name"),
                require(csvRecord, "last_name"),
                email,
                signupDate,
                loyaltyPoints);
    }

    private String require(CSVRecord csvRecord, String columnName) {
        String value = csvRecord.get(columnName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(columnName + " est obligatoire");
        }
        return value.trim();
    }
}
