BEGIN;

-- Create schemafull user table.
DEFINE TABLE user SCHEMAFULL;

-- Define some fields.
DEFINE FIELD firstName ON TABLE user TYPE string
  ASSERT $value != NONE;
DEFINE FIELD lastName ON TABLE user TYPE string
  ASSERT $value != NONE;
DEFINE FIELD email ON TABLE user TYPE string
  ASSERT $value != NONE AND string::is::email($value);

COMMIT;
