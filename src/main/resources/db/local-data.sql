INSERT INTO bets (
    bet_id,
    user_id,
    event_id,
    event_market_id,
    event_winner_id,
    bet_amount,
    status,
    settled_at
) VALUES
    ('bet-001', 'user-001', 'event-123', 'winner', 'team-a', 10.00, 'PENDING', NULL),
    ('bet-002', 'user-002', 'event-123', 'winner', 'team-b', 20.00, 'PENDING', NULL),
    ('bet-003', 'user-003', 'event-123', 'winner', 'team-a', 30.00, 'PENDING', NULL),

    ('bet-004', 'user-004', 'event-456', 'winner', 'team-c', 15.00, 'PENDING', NULL),
    ('bet-005', 'user-005', 'event-456', 'winner', 'team-d', 25.00, 'PENDING', NULL),
    ('bet-006', 'user-006', 'event-456', 'winner', 'team-d', 35.00, 'PENDING', NULL),
    ('bet-007', 'user-007', 'event-456', 'winner', 'team-c', 45.00, 'PENDING', NULL),

    ('bet-008', 'user-008', 'event-789', 'winner', 'team-e', 12.50, 'PENDING', NULL),
    ('bet-009', 'user-009', 'event-789', 'winner', 'team-f', 22.50, 'PENDING', NULL),
    ('bet-010', 'user-010', 'event-789', 'winner', 'team-e', 32.50, 'PENDING', NULL),

    ('bet-011', 'user-011', 'event-101', 'winner', 'team-g', 50.00, 'PENDING', NULL),
    ('bet-012', 'user-012', 'event-101', 'winner', 'team-h', 60.00, 'PENDING', NULL),
    ('bet-013', 'user-013', 'event-101', 'winner', 'team-g', 70.00, 'PENDING', NULL),
    ('bet-014', 'user-014', 'event-101', 'winner', 'team-h', 80.00, 'PENDING', NULL),

    ('bet-015', 'user-015', 'event-202', 'winner', 'team-i', 11.00, 'PENDING', NULL),
    ('bet-016', 'user-016', 'event-202', 'winner', 'team-j', 21.00, 'PENDING', NULL),
    ('bet-017', 'user-017', 'event-202', 'winner', 'team-i', 31.00, 'PENDING', NULL),
    ('bet-018', 'user-018', 'event-202', 'winner', 'team-j', 41.00, 'PENDING', NULL),
    ('bet-019', 'user-019', 'event-202', 'winner', 'team-i', 51.00, 'PENDING', NULL);
