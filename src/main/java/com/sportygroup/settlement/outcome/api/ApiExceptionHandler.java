package com.sportygroup.settlement.outcome.api;

import com.sportygroup.settlement.outcome.service.OutcomePublishException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalidRequest(MethodArgumentNotValidException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "eventId, eventName and eventWinnerId must be non-blank");
        detail.setTitle("Invalid event outcome");
        return detail;
    }

    @ExceptionHandler(OutcomePublishException.class)
    ProblemDetail kafkaUnavailable(OutcomePublishException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Event outcome was not acknowledged by Kafka; retry the request");
        detail.setTitle("Kafka unavailable");
        return detail;
    }
}
