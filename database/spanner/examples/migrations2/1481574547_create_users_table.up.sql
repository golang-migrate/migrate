-- Create a table
CREATE TABLE Users (
  UserId  INT64,
  Name    STRING(40),
  Email   STRING(83)
) PRIMARY KEY(UserId /* even inline comments */);

CREATE UNIQUE INDEX UsersEmailIndex ON Users (Email);

-- Comments are okay

INSERT INTO Users(UserId, Name, Email)
  VALUES
  (100, "Username", "email@domain.com"),
  (200, "Username2", "email2@domain.com");
