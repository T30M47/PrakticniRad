-- CREATE USER postgres WITH REPLICATION ENCRYPTED PASSWORD 'Rea123Teo';
SELECT pg_sleep(10);
SELECT * FROM pg_create_physical_replication_slot('replication_slot');
-- ALTER SYSTEM SET synchronous_standby_names = 'postrges_1_slave';
-- ALTER SYSTEM SET synchronous_commit = on;