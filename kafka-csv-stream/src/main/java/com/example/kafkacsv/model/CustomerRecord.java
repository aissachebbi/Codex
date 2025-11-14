package com.example.kafkacsv.model;

import java.time.LocalDate;

public record CustomerRecord(
        String id,
        String firstName,
        String lastName,
        String email,
        LocalDate signupDate,
        int loyaltyPoints) {
}
