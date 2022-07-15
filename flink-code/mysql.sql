SET sql-client.execution.result-mode=TABLEAU;

CREATE CATALOG my_catalog WITH(
    'type' = 'jdbc',
    'default-database' = 'test',
    'username' = 'sammy',
    'password' = 'password',
    'base-url' = 'jdbc:mysql://localhost:3306'
);

USE CATALOG my_catalog;

SELECT * FROM person;

INSERT INTO person2 SELECT * FROM person;