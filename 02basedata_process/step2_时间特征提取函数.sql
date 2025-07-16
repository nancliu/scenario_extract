-- Step 2: 时间特征提取与日期分类函数
-- 依赖表：dim.dim_holidays(holiday_date, holiday_name, is_free, is_workday)

CREATE OR REPLACE FUNCTION user_extract_time_features(p_timestamp TIMESTAMP)
RETURNS TABLE (
    hour_of_day INTEGER,
    day_of_week INTEGER,
    date DATE,
    pattern_type VARCHAR(50),
    is_workday BOOLEAN,
    is_free_highway BOOLEAN,
    holiday_name VARCHAR(50),
    is_adjusted_workday BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        EXTRACT(HOUR FROM p_timestamp)::INTEGER AS hour_of_day,
        EXTRACT(DOW FROM p_timestamp)::INTEGER AS day_of_week,
        DATE(p_timestamp) AS date,
        -- pattern_type分类逻辑
        (
            CASE
                WHEN h.is_free = true AND h.is_workday = false THEN 'holiday_free'
                WHEN h.is_free = false AND h.is_workday = false THEN 'holiday_nofree'
                WHEN h.is_workday = true THEN 'workday'
                WHEN h.holiday_date IS NULL AND EXTRACT(DOW FROM p_timestamp) IN (0,6) THEN 'weekend'
                ELSE 'workday'
            END
        )::VARCHAR(50) AS pattern_type,
        -- 是否工作日
        CASE
            WHEN h.is_workday = true THEN true
            WHEN h.holiday_date IS NULL AND EXTRACT(DOW FROM p_timestamp) NOT IN (0,6) THEN true
            ELSE false
        END AS is_workday,
        -- 是否免费通行节假日
        CASE WHEN h.is_free = true THEN true ELSE false END AS is_free_highway,
        h.holiday_name::VARCHAR(50),
        -- 是否为调休工作日
        CASE WHEN h.is_workday = true AND EXTRACT(DOW FROM p_timestamp) IN (0,6) THEN true ELSE false END AS is_adjusted_workday
    FROM
        (SELECT * FROM dim.dim_holidays WHERE holiday_date = DATE(p_timestamp)) h
    RIGHT JOIN (SELECT 1) dummy ON true;
END;
$$ LANGUAGE plpgsql;

-- 用法示例：
-- SELECT * FROM user_extract_time_features('2025-01-26 08:00:00');
-- 结果应能正确识别调休、节假日、免费/收费等特征。 