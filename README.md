Used Defined Scalar Function able to export table data

    CREATE TABLE foo (id INT, name VARCHAR(255);
    INSERT INTO foo VALUES (1, 'Alice');
    INSERT INTO foo VALUES (2, 'Bob');
    INSERT INTO foo VALUES (3, 'Cecil');
    INSERT INTO foo VALUES (4, 'Dave');
    
    SELECT export(id, name USING PARAMETERS fpath = '/tmp/foo') FROM foo;
