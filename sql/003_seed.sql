INSERT INTO advertisers (id, name) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Stride Co'),
    ('22222222-2222-2222-2222-222222222222', 'AeroSteps'),
    ('33333333-3333-3333-3333-333333333333', 'TrailPeak');

INSERT INTO campaigns (
    id, advertiser_id, name, headline, landing_url, keywords,
    bid_micros, daily_budget_micros, spent_today_micros, status, geo_targets
) VALUES
    (
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1',
        '11111111-1111-1111-1111-111111111111',
        'Campaign A',
        'Lightweight running shoes for daily miles',
        'https://example.com/stride',
        ARRAY['running', 'shoes', 'lightweight', 'daily'],
        4000000, 500000000, 0, 'active', ARRAY['IN', 'US']
    ),
    (
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2',
        '22222222-2222-2222-2222-222222222222',
        'Campaign B',
        'Pro racing running shoes',
        'https://example.com/aero',
        ARRAY['running', 'shoes', 'racing', 'pro'],
        6000000, 800000000, 0, 'active', ARRAY['IN']
    ),
    (
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3',
        '33333333-3333-3333-3333-333333333333',
        'Campaign C',
        'Trail running shoes for wet weather',
        'https://example.com/trail',
        ARRAY['running', 'shoes', 'trail', 'weather'],
        5000000, 400000000, 0, 'active', ARRAY['IN', 'GB']
    ),
    (
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4',
        '11111111-1111-1111-1111-111111111111',
        'Paused sneakers',
        'Sneakers on sale',
        'https://example.com/paused',
        ARRAY['sneakers', 'sale'],
        3000000, 100000000, 0, 'paused', ARRAY['IN']
    );

INSERT INTO cohort_counts (cohort_id, member_count) VALUES
    ('cohort_runners_in', 1280),
    ('cohort_tiny', 12);
