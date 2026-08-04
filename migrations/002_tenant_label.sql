-- Reverse map for human/org tenant ids (Pg stores uuid5(label)); lets the
-- serving layer hydrate all tenants at boot with an owner connection.
CREATE TABLE IF NOT EXISTS tenant_label (
  tenant_id uuid PRIMARY KEY,
  label     text NOT NULL UNIQUE
);
