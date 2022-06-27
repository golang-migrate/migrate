CREATE TABLE IF NOT EXISTS customers (
     id            INTEGER,
     customer_name VARCHAR[60],
     email         VARCHAR[150],
     address       VARCHAR,
     city          VARCHAR,
     ip            VARCHAR[40],
     country       VARCHAR[15],
     age           INTEGER,
     active        BOOLEAN,
     PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS products (
    id          INTEGER,
    product     VARCHAR NOT NULL,
    price       VARCHAR NOT NULL,
    created_at  TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS orders (
    id          INTEGER AUTO_INCREMENT,
    customerid  INTEGER,
    productid   INTEGER,
    created_at  TIMESTAMP,
    PRIMARY KEY id
);

CREATE TABLE customer_review(
    customerid  INTEGER,
    productid   INTEGER,
    review      VARCHAR,
    created_at  TIMESTAMP,
    PRIMARY KEY (customerid, productid)
);
