ALTER TABLE tusers
    ADD COLUMN last_login TIMESTAMP,
    ADD COLUMN status VARCHAR(20) DEFAULT 'active',
    ADD COLUMN profile_picture VARCHAR(255);