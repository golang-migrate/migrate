CREATE EXTERNAL TABLE IF NOT EXISTS `my_new_schema`.default.books
(
    id          STRING NOT NULL,   -- id of the book
    name        STRING NOT NULL,   -- name of the book
)
    LOCATION 's3://my_external_tables';
