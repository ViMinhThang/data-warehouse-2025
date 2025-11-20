CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER dw_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'dw', port '5432');

CREATE USER MAPPING FOR fragile SERVER dw_server OPTIONS (user 'fragile', password '12333456');