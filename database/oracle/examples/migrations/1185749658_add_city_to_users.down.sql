DECLARE
    v_column_exists number := 0;
BEGIN
    SELECT COUNT(*)
    INTO v_column_exists
    FROM user_tab_cols
    WHERE  table_name = 'USERS'
      AND column_name = 'CITY';

    IF( v_column_exists = 1 )
    THEN
        EXECUTE IMMEDIATE 'ALTER TABLE users DROP COLUMN CITY';
    END IF;
END;