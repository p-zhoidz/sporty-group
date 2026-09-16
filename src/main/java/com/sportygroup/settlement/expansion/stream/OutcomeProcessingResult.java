package com.sportygroup.settlement.expansion.stream;

record OutcomeProcessingResult(Route route, String payload) {

    enum Route {
        PAGE_TASK,
        DLT
    }
}
