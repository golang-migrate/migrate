CREATE INDEX ON customers(customer_name);
CREATE INDEX ON customers(country, ip);
CREATE INDEX IF NOT EXISTS ON customers(active);
CREATE UNIQUE INDEX ON customers(email);
