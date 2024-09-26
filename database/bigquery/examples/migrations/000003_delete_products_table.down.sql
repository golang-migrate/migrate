CREATE TABLE IF NOT EXISTS test_dataset_id.products (
    id          INT64,
    name        STRING
);

INSERT test_dataset_id.products (id, name)
VALUES (1, "name1"),
       (2, "name2"),
       (3, "name3");