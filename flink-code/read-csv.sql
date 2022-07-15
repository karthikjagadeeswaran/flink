SET 'execution.runtime-mode' = 'batch';

CREATE TABLE table1 (
          sym STRING,
         dt STRING,
        ts STRING,
        open1 DOUBLE,
       high DOUBLE,
       low DOUBLE,
       close1 DOUBLE,
       oi STRING
    ) WITH (
        'connector.type' = 'filesystem',
        'format.type' = 'csv',
        'connector.path' = 'file:///home/karthik/work/testdir/27MAY/ACC.txt')

