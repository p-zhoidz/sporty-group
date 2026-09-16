package com.sportygroup.settlement.outcome.api;

import com.sportygroup.settlement.outcome.service.AcceptOutcomeService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/event-outcomes")
public class EventOutcomeController {

    private final AcceptOutcomeService acceptOutcomeService;

    public EventOutcomeController(AcceptOutcomeService acceptOutcomeService) {
        this.acceptOutcomeService = acceptOutcomeService;
    }

    @PostMapping
    public ResponseEntity<EventOutcomeResponse> publish(@Valid @RequestBody EventOutcomeRequest request) {
        acceptOutcomeService.accept(request.toDomain());
        return ResponseEntity.accepted()
                .body(new EventOutcomeResponse(request.eventId(), "ACCEPTED"));
    }
}
