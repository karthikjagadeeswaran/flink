-- set result-mode --
SET sql-client.execution.result-mode=TABLEAU;

-- create catalog source from jdbc --
CREATE CATALOG my_catalog WITH(
    'type' = 'jdbc',
    'default-database' = 'test',
    'username' = 'postgres',
    'password' = 'postgres',
    'base-url' = 'jdbc:postgresql://localhost:5432'
);

-- use custom catalog --
USE CATALOG my_catalog;

-- transfer data from source into sink without create them --
INSERT into table2 SELECT name FROM table1;
