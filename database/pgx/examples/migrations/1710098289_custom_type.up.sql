CREATE TYPE "custom_user_type" AS ENUM('foo', 'bar', 'qux');

CREATE TABLE "custom_users" (
  "user_id"   integer unique,
  "name"      text,
  "email"     text,
  "user_type" custom_user_type
);
