-- One column. No created_at: a timestamp would let anyone holding this table
-- correlate enrollment times against credential-issuance times and deanonymize
-- students. No serial id: insertion order leaks enrollment sequence.
CREATE TABLE IF NOT EXISTS nullifier (n BYTEA PRIMARY KEY);

-- The auth role can add to the set but cannot read it. `INSERT .. ON CONFLICT
-- DO NOTHING` needs no SELECT privilege, and freshness comes from the affected
-- row count rather than RETURNING. A fully compromised auth server therefore
-- cannot enumerate the enrollment set.
--
-- Careful: ON CONFLICT DO UPDATE *does* require SELECT. Never migrate to it.
-- Verify with \dp nullifier after applying.
REVOKE ALL ON nullifier FROM PUBLIC;
GRANT INSERT ON nullifier TO auth;
