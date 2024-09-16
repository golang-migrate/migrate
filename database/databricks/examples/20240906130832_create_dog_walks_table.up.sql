CREATE EXTERNAL TABLE IF NOT EXISTS `dog-${env}-park-db`.default.dog_walks (
    path        STRING NOT NULL,      -- Absolute path where the walk info is stored
    num_steps   LONG NOT NULL,        -- Number of steps in the walk
    walk_time   TIMESTAMP NOT NULL,   -- When the walk happened
    checkpoint_id TIMESTAMP NOT NULL, -- ID given to the batch per checkpoint, assigned to many process runs.
    batch_id    TIMESTAMP NOT NULL,   -- ID given to each independent batch, assigned once per process run
    recorded_at TIMESTAMP NOT NULL    -- Timestamp indicating when the walk was recorded.
) LOCATION 's3://dog-${env}-park-db-tables/dog_walks';
