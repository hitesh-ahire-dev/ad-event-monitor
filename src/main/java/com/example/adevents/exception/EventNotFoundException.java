package com.example.adevents.exception;

/** Thrown when no enriched record is found for a given eventId. */
public class EventNotFoundException extends RuntimeException {
    public EventNotFoundException(String message) {
        super(message);
    }
}
