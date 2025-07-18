-- OD表查询优化建议SQL脚本
-- 基于性能测试结果的优化建议

-- 1. 检查当前索引使用情况
SELECT 
    indexrelname as index_name,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    idx_scan::float / GREATEST(seq_scan, 1) as index_vs_seq_ratio
FROM pg_stat_user_indexes 
WHERE schemaname = 'dwd' 
  AND relname = 'dwd_od_weekly'
ORDER BY idx_scan DESC;

-- 2. 建议的复合索引（如果查询经常按车辆类型过滤）
-- CREATE INDEX CONCURRENTLY idx_dwd_od_weekly_time_vehicle 
-- ON dwd.dwd_od_weekly (start_time, vehicle_type);

-- 3. 建议的复合索引（如果经常查询特定的起终点组合）
-- CREATE INDEX CONCURRENTLY idx_dwd_od_weekly_od_pair 
-- ON dwd.dwd_od_weekly (start_square_code, end_square_code, start_time);

-- 4. 建议的部分索引（只索引最近的数据）
-- CREATE INDEX CONCURRENTLY idx_dwd_od_weekly_recent 
-- ON dwd.dwd_od_weekly (start_time) 
-- WHERE start_time >= CURRENT_DATE - INTERVAL '30 days';

-- 5. 检查分区剪枝效果
EXPLAIN (ANALYZE, BUFFERS) 
SELECT COUNT(*) 
FROM dwd.dwd_od_weekly 
WHERE start_time >= '2025-07-07' 
  AND start_time < '2025-07-13';

-- 6. 优化的查询示例（避免ORDER BY）
EXPLAIN (ANALYZE, BUFFERS) 
SELECT 
    pass_id,
    vehicle_type,
    start_time,
    start_square_code,
    end_square_code
FROM dwd.dwd_od_weekly 
WHERE start_time >= '2025-07-07' 
  AND start_time < '2025-07-13'
LIMIT 10000;

-- 7. 分页查询示例
-- 第一页
SELECT 
    pass_id,
    vehicle_type,
    start_time,
    start_square_code,
    end_square_code
FROM dwd.dwd_od_weekly 
WHERE start_time >= '2025-07-07' 
  AND start_time < '2025-07-13'
ORDER BY start_time, pass_id
LIMIT 10000;

-- 后续页（使用游标）
-- SELECT ... WHERE start_time >= '2025-07-07' 
--   AND (start_time > '上一页最后的start_time' 
--        OR (start_time = '上一页最后的start_time' AND pass_id > '上一页最后的pass_id'))
-- ORDER BY start_time, pass_id
-- LIMIT 10000;

-- 8. 检查表统计信息是否最新
SELECT 
    schemaname,
    tablename,
    last_analyze,
    last_autoanalyze,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM pg_stat_user_tables 
WHERE schemaname = 'dwd' 
  AND relname = 'dwd_od_weekly';

-- 9. 如果统计信息过期，手动更新
-- ANALYZE dwd.dwd_od_weekly;

-- 10. 检查分区信息（PostgreSQL 10+）
SELECT 
    schemaname,
    tablename,
    partitionboundary
FROM pg_partitions 
WHERE schemaname = 'dwd' 
  AND tablename = 'dwd_od_weekly'
ORDER BY partitionboundary;

-- 或者使用新的系统表（PostgreSQL 10+）
SELECT 
    n.nspname as schema_name,
    c.relname as table_name,
    pg_get_expr(c.relpartbound, c.oid) as partition_expression
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relispartition 
  AND n.nspname = 'dwd'
  AND c.relname LIKE 'dwd_od_weekly%'
ORDER BY c.relname;
