package com.sportygroup.settlement.bet.service;

import com.sportygroup.settlement.bet.model.BetProjection;
import com.sportygroup.settlement.bet.repository.BetRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class BetQueryService {

    private final BetRepository repository;

    public BetQueryService(BetRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<BetProjection> findPage(String eventId, String afterBetId, int pageSize) {
        PageRequest page = PageRequest.of(0, pageSize);
        if (afterBetId == null) {
            return repository.findByEventIdOrderByBetIdAsc(eventId, page);
        }
        return repository.findByEventIdAndBetIdGreaterThanOrderByBetIdAsc(
                eventId, afterBetId, page);
    }
}
