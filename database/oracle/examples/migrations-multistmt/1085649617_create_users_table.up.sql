CREATE TABLE USERS_MS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
);

---

DROP TABLE IF EXISTS USERS_MS;

---

CREATE TABLE USERS_MS (
   USER_ID integer unique,
   NAME    varchar(40),
   EMAIL   varchar(40)
);