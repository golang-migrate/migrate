CREATE TABLE Transactions (
  UserId        INT64,
  TransactionId STRING(40),
  Total         NUMERIC
) PRIMARY KEY(UserId, TransactionId), 
INTERLEAVE IN PARENT Users ON DELETE CASCADE
