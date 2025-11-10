-- Trino doesn't support traditional indexes, but we can create a view for common queries
CREATE OR REPLACE VIEW user_by_email AS
SELECT id, name, email, created_at
FROM users
WHERE email IS NOT NULL