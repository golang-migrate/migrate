CREATE TABLE users (
    id uuid PRIMARY KEY,
    email varchar(255) NOT NULL,
    is_active boolean NOT NULL DEFAULT 1,
    created_at timestamp NOT NULL,
    metadata json
) STRICT;

CREATE INDEX idx_users_email ON users (email);
