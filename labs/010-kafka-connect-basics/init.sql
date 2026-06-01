CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE NOT NULL
);

ALTER TABLE users REPLICA IDENTITY FULL;

INSERT INTO users (first_name, last_name, email) VALUES ('Alice', 'Smith', 'alice@example.com');
INSERT INTO users (first_name, last_name, email) VALUES ('Bob', 'Jones', 'bob@example.com');
