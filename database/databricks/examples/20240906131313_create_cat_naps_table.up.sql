CREATE EXTERNAL TABLE IF NOT EXISTS `dog-park-db`.default.cat_naps (
    nap_id            STRING NOT NULL,    -- id of the nap
    nap_location      STRING NOT NULL,    -- location where the nap took place
    checkpoint_id LONG NOT NULL,          -- ID given to the batch per checkpoint, assigned to many process runs.
    batch_id    STRING NOT NULL,          -- ID given to each independent batch
    recorded_at        TIMESTAMP NOT NULL -- Timestamp indicating when the nap was recorded.
) LOCATION 's3://dog-park-db-tables/cat_naps';
