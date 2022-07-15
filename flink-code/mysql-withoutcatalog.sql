-- set result-mode --
SET sql-client.execution.result-mode=TABLEAU;

--- create source table with jdbc connector ---
CREATE TABLE table1 (
    PersonID INT,
    Name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test?user=sammy&password=password',
    'table-name' = 'person'
);

--- create sink table with jdbc connector ---
CREATE TABLE table2 (
    PersonID INT,
    Name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test?user=sammy&password=password',
    'table-name' = 'person2'
);

--- transfer data from source to sink --
INSERT into table2 SELECT * FROM table1;
