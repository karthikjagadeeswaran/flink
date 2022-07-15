-- set result-mode --
SET sql-client.execution.result-mode=TABLEAU;

--- create source table with jdbc connector ---
CREATE TABLE table1 (
    PersonId INT,
    Name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/test?user=postgres&password=postgres',
    'table-name' = 'table1'
);

--- create sink table with jdbc connector ---
CREATE TABLE table2 (
    PersonId INT,
    Name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/test?user=postgres&password=postgres',
    'table-name' = 'table2'
);

--- transfer data from source to sink --
INSERT into table2 SELECT * FROM table1;
