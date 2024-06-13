ALTER TABLE pets ADD predator bool;

CREATE TABLE pets_with_fk (
  foreign_key_column_name INTEGER,
  FOREIGN KEY(foreign_key_column_name) REFERENCES pets(id)
);