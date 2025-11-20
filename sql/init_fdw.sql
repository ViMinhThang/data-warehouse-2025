-- =============================================
-- 0. LOGIN SUPERUSER trước khi chạy script này
-- =============================================
-- sudo -u postgres psql -d dm

-- =============================================
-- 1. CLEAN UP (chỉ dùng nếu muốn làm lại hoàn toàn)
-- =============================================

-- Drop user mapping fragile nếu tồn tại
DO $$
BEGIN
   IF EXISTS (
       SELECT 1
       FROM pg_user_mappings
       WHERE umuser = (SELECT oid FROM pg_roles WHERE rolname='fragile')
         AND srvname = 'dw_server'
   ) THEN
       EXECUTE 'DROP USER MAPPING FOR fragile SERVER dw_server';
   END IF;
END$$;

-- Drop server nếu tồn tại
DROP SERVER IF EXISTS dw_server CASCADE;

-- Drop schema dm_dw nếu muốn làm lại
DROP SCHEMA IF EXISTS dm_dw CASCADE;

-- =============================================
-- 2. Tạo extension postgres_fdw
-- =============================================
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- =============================================
-- 3. Tạo server FDW
-- =============================================
CREATE SERVER dw_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'dw',
    port '5432'
);

-- =============================================
-- 4. Tạo user mapping cho fragile
-- =============================================
CREATE USER MAPPING FOR fragile
SERVER dw_server
OPTIONS (user 'fragile', password '123456');

-- =============================================
-- 5. Tạo schema dm_dw
-- =============================================
CREATE SCHEMA IF NOT EXISTS dm_dw;

-- Grant quyền cần thiết cho fragile
GRANT USAGE ON SCHEMA dm_dw TO fragile;
GRANT CREATE ON SCHEMA dm_dw TO fragile;

-- Grant quyền dùng server FDW
GRANT USAGE ON FOREIGN SERVER dw_server TO fragile;

-- =============================================
-- 6. IMPORT bảng từ DW (chạy **bằng user fragile**)
-- =============================================
-- Lưu ý: Logout postgres, login bằng fragile:
-- psql -U fragile -d dm
IMPORT FOREIGN SCHEMA public
FROM SERVER dw_server
INTO dm_dw;

-- =============================================
-- 7. Kiểm tra foreign tables
-- =============================================
\det dm_dw.*
