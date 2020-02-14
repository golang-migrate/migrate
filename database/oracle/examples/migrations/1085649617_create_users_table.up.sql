CREATE TABLE USERS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
);

---

BEGIN
EXECUTE IMMEDIATE 'DROP TABLE USERS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;

---

CREATE TABLE USERS (
   USER_ID integer unique,
   NAME    varchar(40),
   EMAIL   varchar(40)
);
