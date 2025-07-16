-- Step 3: 基准流量数据批次统计表及维护脚本

-- 1. 创建统计表
CREATE TABLE IF NOT EXISTS baseline.baseflow_batch_stat (
    id serial PRIMARY KEY,
    batch_id varchar(50) NOT NULL,
    data_type varchar(32) NOT NULL, -- 新增，标识数据类型
    start_time timestamp NOT NULL,
    end_time timestamp NOT NULL,
    generate_time timestamp NOT NULL DEFAULT now(),
    data_count int NOT NULL,        -- 本类型数据量
    is_latest boolean NOT NULL DEFAULT false,
    remark varchar(200)
);



-- 先清空统计表（如需保留历史请注释此行）
TRUNCATE TABLE baseline.baseflow_batch_stat;

-- 门架
INSERT INTO baseline.baseflow_batch_stat (
    batch_id, data_type, start_time, end_time, data_count, is_latest, remark
)
SELECT
    batch_id,
    'gantry',
    TO_DATE(SPLIT_PART(batch_id, '_', 1), 'YYYYMMDD'),
    TO_DATE(SPLIT_PART(batch_id, '_', 2), 'YYYYMMDD') + INTERVAL '23:59:59',
    COUNT(*),
    false,
    '初始化批量导入'
FROM baseline.baseflow_pattern_gantry
GROUP BY batch_id;

-- 入口
INSERT INTO baseline.baseflow_batch_stat (
    batch_id, data_type, start_time, end_time, data_count, is_latest, remark
)
SELECT
    batch_id,
    'tollsquare_on',
    TO_DATE(SPLIT_PART(batch_id, '_', 1), 'YYYYMMDD'),
    TO_DATE(SPLIT_PART(batch_id, '_', 2), 'YYYYMMDD') + INTERVAL '23:59:59',
    COUNT(*),
    false,
    '初始化批量导入'
FROM baseline.baseflow_pattern_tollsquare_on
GROUP BY batch_id;

-- 出口
INSERT INTO baseline.baseflow_batch_stat (
    batch_id, data_type, start_time, end_time, data_count, is_latest, remark
)
SELECT
    batch_id,
    'tollsquare_off',
    TO_DATE(SPLIT_PART(batch_id, '_', 1), 'YYYYMMDD'),
    TO_DATE(SPLIT_PART(batch_id, '_', 2), 'YYYYMMDD') + INTERVAL '23:59:59',
    COUNT(*),
    false,
    '初始化批量导入'
FROM baseline.baseflow_pattern_tollsquare_off
GROUP BY batch_id;

-- OD
INSERT INTO baseline.baseflow_batch_stat (
    batch_id, data_type, start_time, end_time, data_count, is_latest, remark
)
SELECT
    batch_id,
    'od',
    TO_DATE(SPLIT_PART(batch_id, '_', 1), 'YYYYMMDD'),
    TO_DATE(SPLIT_PART(batch_id, '_', 2), 'YYYYMMDD') + INTERVAL '23:59:59',
    COUNT(*),
    false,
    '初始化批量导入'
FROM baseline.baseflow_pattern_od
GROUP BY batch_id;

-- 以批次号为准
UPDATE baseline.baseflow_batch_stat t
SET is_latest = true
FROM (
    SELECT data_type, MAX(batch_id) AS max_batch
    FROM baseline.baseflow_batch_stat
    GROUP BY data_type
) s
WHERE t.data_type = s.data_type AND t.batch_id = s.max_batch;
-- 如需自动维护`is_latest`为最新批次，或有其它字段/逻辑调整，请随时告知！ 