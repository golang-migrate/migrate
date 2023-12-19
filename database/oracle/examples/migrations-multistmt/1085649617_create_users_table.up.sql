CREATE TABLE USERS_MS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
);

---

BEGIN
EXECUTE IMMEDIATE 'DROP TABLE USERS_MS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;

---

CREATE TABLE USERS_MS (
   USER_ID integer unique,
   NAME    varchar(40),
   EMAIL   varchar(40)
);
