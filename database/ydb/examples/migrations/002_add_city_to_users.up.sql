CREATE TABLE `test/cities` (
    id Uint64,
    name String,
    PRIMARY KEY (id)
);

ALTER TABLE `test/users` ADD COLUMN city Uint64;
