BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX users_email_index';
EXCEPTION
    WHEN OTHERS THEN
        -- ORA-01418: specified index does not exist
        IF SQLCODE != -1418 THEN
            RAISE;
        END IF;
END;
