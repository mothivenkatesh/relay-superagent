-- gate latency in ms can exceed int4 when cards span days/weeks (and demo
-- world-clock jumps); store as bigint.
ALTER TABLE run ALTER COLUMN gate_latency_ms TYPE bigint;
