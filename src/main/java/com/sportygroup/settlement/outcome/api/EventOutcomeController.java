package com.sportygroup.settlement.outcome.api;

import com.sportygroup.settlement.outcome.service.AcceptOutcomeService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/event-outcomes")
public class EventOutcomeController {

    private static final Logger log = LoggerFactory.getLogger(EventOutcomeController.class);

    private final AcceptOutcomeService acceptOutcomeService;

    public EventOutcomeController(AcceptOutcomeService acceptOutcomeService) {
        this.acceptOutcomeService = acceptOutcomeService;
    }

    @PostMapping
    public ResponseEntity<EventOutcomeResponse> publish(@Valid @RequestBody EventOutcomeRequest request) {
        log.info(
                "[EVENT_OUTCOME_RECEIVED][EVENT_ID: {}][WINNER_ID: {}]",
                request.eventId(), request.eventWinnerId());
        acceptOutcomeService.accept(request.toDomain());
        log.info("[EVENT_OUTCOME_ACCEPTED][EVENT_ID: {}]", request.eventId());
        return ResponseEntity.accepted()
                .body(new EventOutcomeResponse(request.eventId(), "ACCEPTED"));
    }
}
