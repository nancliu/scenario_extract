-- Step 3: 周分区表基础流量聚合处理函数
-- 依赖：user_extract_time_features

-- 1. 门架流量基准初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_weekly_gantry_baseflow(p_batch_id VARCHAR DEFAULT NULL, p_start_time TIMESTAMP DEFAULT NULL, p_end_time TIMESTAMP DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    -- 获取时间范围
    IF p_start_time IS NULL OR p_end_time IS NULL THEN
        SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_flow_gantry_weekly;
    ELSE
        v_min_time := p_start_time;
        v_max_time := p_end_time;
    END IF;
    IF p_batch_id IS NULL THEN
        v_batch_id := TO_CHAR(v_min_time, 'YYYYMMDD') || '_' || TO_CHAR(v_max_time, 'YYYYMMDD');
    ELSE
        v_batch_id := p_batch_id;
    END IF;

    -- 删除已存在的同批次数据，避免重复
    DELETE FROM baseline.baseflow_pattern_gantry WHERE batch_id = v_batch_id;

    -- 插入聚合结果
    INSERT INTO baseline.baseflow_pattern_gantry (
        gantry_id, pattern_type, hour, batch_id,
        mean, stddev, median, p75, p85, data_points_count,
        passenger_small, passenger_large, truck_small, truck_large, special_small, special_large, truck_ratio,
        update_time
    )
    SELECT
        f.start_gantryid AS gantry_id,
        t.pattern_type,
        t.hour_of_day,
        v_batch_id AS batch_id,
        AVG(COALESCE(f.total,0)) AS mean,
        STDDEV_POP(COALESCE(f.total,0)) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS p85,
        COUNT(*) AS data_points_count,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(COALESCE(f.total,0)) = 0 THEN 0 ELSE SUM(COALESCE(f.total_h,0) + COALESCE(f.total_t,0))::NUMERIC / NULLIF(SUM(COALESCE(f.total,0)),0) END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_flow_gantry_weekly f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.start_gantryid, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 2. 收费广场入口流量基准初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_weekly_tollsquare_on_baseflow(p_batch_id VARCHAR DEFAULT NULL, p_start_time TIMESTAMP DEFAULT NULL, p_end_time TIMESTAMP DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    IF p_start_time IS NULL OR p_end_time IS NULL THEN
        SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_flow_onramp_weekly;
    ELSE
        v_min_time := p_start_time;
        v_max_time := p_end_time;
    END IF;
    IF p_batch_id IS NULL THEN
        v_batch_id := TO_CHAR(v_min_time, 'YYYYMMDD') || '_' || TO_CHAR(v_max_time, 'YYYYMMDD');
    ELSE
        v_batch_id := p_batch_id;
    END IF;

    DELETE FROM baseline.baseflow_pattern_tollsquare_on WHERE batch_id = v_batch_id;

    INSERT INTO baseline.baseflow_pattern_tollsquare_on (
        square_code, pattern_type, hour, batch_id,
        mean, stddev, median, p75, p85, data_points_count,
        passenger_small, passenger_large, truck_small, truck_large, special_small, special_large, truck_ratio,
        update_time
    )
    SELECT
        f.square_code,
        t.pattern_type,
        t.hour_of_day,
        v_batch_id AS batch_id,
        AVG(COALESCE(f.total,0)) AS mean,
        STDDEV_POP(COALESCE(f.total,0)) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS p85,
        COUNT(*) AS data_points_count,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(COALESCE(f.total,0)) = 0 THEN 0 ELSE SUM(COALESCE(f.total_h,0) + COALESCE(f.total_t,0))::NUMERIC / NULLIF(SUM(COALESCE(f.total,0)),0) END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_flow_onramp_weekly f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.square_code, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 3. 收费广场出口流量基准初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_weekly_tollsquare_off_baseflow(p_batch_id VARCHAR DEFAULT NULL, p_start_time TIMESTAMP DEFAULT NULL, p_end_time TIMESTAMP DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    IF p_start_time IS NULL OR p_end_time IS NULL THEN
        SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_flow_offramp_weekly;
    ELSE
        v_min_time := p_start_time;
        v_max_time := p_end_time;
    END IF;
    IF p_batch_id IS NULL THEN
        v_batch_id := TO_CHAR(v_min_time, 'YYYYMMDD') || '_' || TO_CHAR(v_max_time, 'YYYYMMDD');
    ELSE
        v_batch_id := p_batch_id;
    END IF;

    DELETE FROM baseline.baseflow_pattern_tollsquare_off WHERE batch_id = v_batch_id;

    INSERT INTO baseline.baseflow_pattern_tollsquare_off (
        square_code, pattern_type, hour, batch_id,
        mean, stddev, median, p75, p85, data_points_count,
        passenger_small, passenger_large, truck_small, truck_large, special_small, special_large, truck_ratio,
        update_time
    )
    SELECT
        f.square_code,
        t.pattern_type,
        t.hour_of_day,
        v_batch_id AS batch_id,
        AVG(COALESCE(f.total,0)) AS mean,
        STDDEV_POP(COALESCE(f.total,0)) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY COALESCE(f.total,0)) AS p85,
        COUNT(*) AS data_points_count,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(COALESCE(f.total,0)) = 0 THEN 0 ELSE SUM(COALESCE(f.total_h,0) + COALESCE(f.total_t,0))::NUMERIC / NULLIF(SUM(COALESCE(f.total,0)),0) END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_flow_offramp_weekly f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.square_code, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 4. OD基准流量初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_weekly_od_baseflow(p_batch_id VARCHAR DEFAULT NULL, p_start_time TIMESTAMP DEFAULT NULL, p_end_time TIMESTAMP DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    IF p_start_time IS NULL OR p_end_time IS NULL THEN
        SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_od_weekly;
    ELSE
        v_min_time := p_start_time;
        v_max_time := p_end_time;
    END IF;
    IF p_batch_id IS NULL THEN
        v_batch_id := TO_CHAR(v_min_time, 'YYYYMMDD') || '_' || TO_CHAR(v_max_time, 'YYYYMMDD');
    ELSE
        v_batch_id := p_batch_id;
    END IF;

    DELETE FROM baseline.baseflow_pattern_od WHERE batch_id = v_batch_id;

    INSERT INTO baseline.baseflow_pattern_od (
        origin_id, destination_id, origin_type, destination_type, od_type_combination,
        pattern_type, hour, batch_id,
        mean, stddev, median, p75, p85, data_points_count, avg_travel_time,
        passenger_small, passenger_large, truck_small, truck_large, special_small, special_large, truck_ratio,
        update_time
    )
    SELECT
        origin_id,
        destination_id,
        origin_type,
        destination_type,
        od_type_combination,
        pattern_type,
        hour,
        v_batch_id AS batch_id,
        AVG(od_flow) AS mean,
        STDDEV_POP(od_flow) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY od_flow) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY od_flow) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY od_flow) AS p85,
        COUNT(*) AS data_points_count,
        AVG(avg_travel_time) AS avg_travel_time,
        AVG(passenger_small) AS passenger_small,
        AVG(passenger_large) AS passenger_large,
        AVG(truck_small) AS truck_small,
        AVG(truck_large) AS truck_large,
        AVG(special_small) AS special_small,
        AVG(special_large) AS special_large,
        CASE WHEN SUM(od_flow) = 0 THEN 0 ELSE SUM(truck_small + truck_large + special_small + special_large)::NUMERIC / NULLIF(SUM(od_flow),0) END AS truck_ratio,
        NOW() AS update_time
    FROM (
        SELECT
            COALESCE(f.start_square_code, f.start_station_code) AS origin_id,
            COALESCE(f.end_square_code, f.end_station_code) AS destination_id,
            CASE 
                WHEN f.start_square_code IS NOT NULL THEN 'tollsquare'
                WHEN f.start_station_code IS NOT NULL THEN 'gantry'
                ELSE 'unknown'
            END AS origin_type,
            CASE 
                WHEN f.end_square_code IS NOT NULL THEN 'tollsquare'
                WHEN f.end_station_code IS NOT NULL THEN 'gantry'
                ELSE 'unknown'
            END AS destination_type,
            CASE 
                WHEN f.start_square_code IS NOT NULL AND f.end_square_code IS NOT NULL THEN 'tollsquare-tollsquare'
                WHEN f.start_station_code IS NOT NULL AND f.end_station_code IS NOT NULL THEN 'gantry-gantry'
                WHEN f.start_square_code IS NOT NULL AND f.end_station_code IS NOT NULL THEN 'tollsquare-gantry'
                WHEN f.start_station_code IS NOT NULL AND f.end_square_code IS NOT NULL THEN 'gantry-tollsquare'
                ELSE 'mixed'
            END AS od_type_combination,
            t.pattern_type,
            t.hour_of_day AS hour,
            COUNT(*) AS od_flow,
            AVG(EXTRACT(EPOCH FROM (f.end_time - f.start_time))) AS avg_travel_time,
            SUM(CASE WHEN f.vehicle_type IN ('k1','k2') THEN 1 ELSE 0 END) AS passenger_small,
            SUM(CASE WHEN f.vehicle_type IN ('k3','k4') THEN 1 ELSE 0 END) AS passenger_large,
            SUM(CASE WHEN f.vehicle_type IN ('h1','h2') THEN 1 ELSE 0 END) AS truck_small,
            SUM(CASE WHEN f.vehicle_type IN ('h3','h4','h5','h6') THEN 1 ELSE 0 END) AS truck_large,
            SUM(CASE WHEN f.vehicle_type IN ('t1','t2') THEN 1 ELSE 0 END) AS special_small,
            SUM(CASE WHEN f.vehicle_type IN ('t3','t4','t5','t6') THEN 1 ELSE 0 END) AS special_large
        FROM dwd.dwd_od_weekly f
        JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
        WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
          AND f.end_time >= f.start_time
        GROUP BY 
            COALESCE(f.start_square_code, f.start_station_code), 
            COALESCE(f.end_square_code, f.end_station_code), 
            t.pattern_type, 
            t.hour_of_day,
            f.start_square_code, f.start_station_code, f.end_square_code, f.end_station_code
    ) sub
    GROUP BY 
        origin_id, destination_id, origin_type, destination_type, od_type_combination, pattern_type, hour;
END;
$$ LANGUAGE plpgsql;


### 1. 逻辑说明

- “当前日期之前完整四周”指的是：**不包含今天**，往前推4个完整的自然周（周一~周日）。
- 假设今天为`2025-07-10`（周四），则最近完整四周为：
  - 第一周：2025-06-09（周一） ~ 2025-06-15（周日）
  - 第二周：2025-06-16（周一） ~ 2025-06-22（周日）
  - 第三周：2025-06-23（周一） ~ 2025-06-29（周日）
  - 第四周：2025-06-30（周一） ~ 2025-07-06（周日）

---

### 2. SQL调用示例

---

####    1. 2025年6月9日开始，滚动1个月，每周输出一个批次号的SQL示例

假设你要从2025-06-09（周一）开始，每周滚动一次，窗口长度为1个月（即每个批次号覆盖4周），每次向前推进一周，直到当前日期（假设当前为2025-08-01）。

### 示例批次号及时间窗口

| 批次号                | 起始日期      | 结束日期         |
|----------------------|--------------|------------------|
| 20250609_20250706    | 2025-06-09   | 2025-07-06 23:59:59 |
| 20250616_20250713    | 2025-06-16   | 2025-07-13 23:59:59 |
| 20250623_20250720    | 2025-06-23   | 2025-07-20 23:59:59 |
| 20250630_20250727    | 2025-06-30   | 2025-07-27 23:59:59 |
| 20250707_20250803    | 2025-07-07   | 2025-08-03 23:59:59 |

### 每周滚动输出SQL示例

```sql
-- 第一批次
SELECT user_generate_weekly_gantry_baseflow('20250609_20250706', '2025-06-09', '2025-07-06 23:59:59');
SELECT user_generate_weekly_tollsquare_on_baseflow('20250609_20250706', '2025-06-09', '2025-07-06 23:59:59');
SELECT user_generate_weekly_tollsquare_off_baseflow('20250609_20250706', '2025-06-09', '2025-07-06 23:59:59');
SELECT user_generate_weekly_od_baseflow('20250609_20250706', '2025-06-09', '2025-07-06 23:59:59');

-- 第二批次
SELECT user_generate_weekly_gantry_baseflow('20250616_20250713', '2025-06-16', '2025-07-13 23:59:59');
SELECT user_generate_weekly_tollsquare_on_baseflow('20250616_20250713', '2025-06-16', '2025-07-13 23:59:59');
SELECT user_generate_weekly_tollsquare_off_baseflow('20250616_20250713', '2025-06-16', '2025-07-13 23:59:59');
SELECT user_generate_weekly_od_baseflow('20250616_20250713', '2025-06-16', '2025-07-13 23:59:59');

-- 第三批次
SELECT user_generate_weekly_gantry_baseflow('20250623_20250720', '2025-06-23', '2025-07-20 23:59:59');
SELECT user_generate_weekly_tollsquare_on_baseflow('20250623_20250720', '2025-06-23', '2025-07-20 23:59:59');
SELECT user_generate_weekly_tollsquare_off_baseflow('20250623_20250720', '2025-06-23', '2025-07-20 23:59:59');
SELECT user_generate_weekly_od_baseflow('20250623_20250720', '2025-06-23', '2025-07-20 23:59:59');

-- 以此类推，每周向前滚动一周
```

---


-- 动态生成当前日期前4个完整周（1个月）基准流量的函数
CREATE OR REPLACE FUNCTION user_generate_latest_4weeks_baseflow()
RETURNS VOID AS $$
DECLARE
    v_today DATE := CURRENT_DATE;
    v_last_sunday DATE;
    v_first_monday DATE;
    v_batch_id TEXT;
    v_start_time TEXT;
    v_end_time TEXT;
    v_start_ts TIMESTAMP;
    v_end_ts TIMESTAMP;
    v_gantry_count INT;
    v_on_count INT;
    v_off_count INT;
    v_od_count INT;
    v_exists INT;
BEGIN
    -- 找到当前日期前的最近一个周日（不含今天）
    v_last_sunday := v_today - ((EXTRACT(DOW FROM v_today)::INTEGER + 0) % 7) - 1;
    -- 找到4周前的周一
    v_first_monday := v_last_sunday - INTERVAL '27 days' + INTERVAL '1 day';
    -- 生成批次号
    v_batch_id := TO_CHAR(v_first_monday, 'YYYYMMDD') || '_' || TO_CHAR(v_last_sunday, 'YYYYMMDD');
    v_start_time := TO_CHAR(v_first_monday, 'YYYY-MM-DD');
    v_end_time := TO_CHAR(v_last_sunday, 'YYYY-MM-DD') || ' 23:59:59';
    v_start_ts := v_first_monday;
    v_end_ts := v_last_sunday + INTERVAL '23:59:59';

    -- 检查统计表是否已存在该批次（任意类型）
    SELECT COUNT(*) INTO v_exists FROM baseline.baseflow_batch_stat WHERE batch_id = v_batch_id;
    IF v_exists > 0 THEN
        RAISE EXCEPTION '批次 % 已存在于baseline.baseflow_batch_stat，已存在前4周的基准流量数据，禁止重复生成！', v_batch_id;
    END IF;

    RAISE NOTICE '批次号: %, 起始: %, 结束: %', v_batch_id, v_start_time, v_end_time;

    -- 门架
    PERFORM user_generate_weekly_gantry_baseflow(v_batch_id, v_start_time, v_end_time);
    -- 入口
    PERFORM user_generate_weekly_tollsquare_on_baseflow(v_batch_id, v_start_time, v_end_time);
    -- 出口
    PERFORM user_generate_weekly_tollsquare_off_baseflow(v_batch_id, v_start_time, v_end_time);
    -- OD
    PERFORM user_generate_weekly_od_baseflow(v_batch_id, v_start_time, v_end_time);

    -- 统计数量
    SELECT COUNT(*) INTO v_gantry_count FROM baseline.baseflow_pattern_gantry WHERE batch_id = v_batch_id;
    SELECT COUNT(*) INTO v_on_count FROM baseline.baseflow_pattern_tollsquare_on WHERE batch_id = v_batch_id;
    SELECT COUNT(*) INTO v_off_count FROM baseline.baseflow_pattern_tollsquare_off WHERE batch_id = v_batch_id;
    SELECT COUNT(*) INTO v_od_count FROM baseline.baseflow_pattern_od WHERE batch_id = v_batch_id;

    -- 维护is_latest标记并插入统计表
    -- 门架
    UPDATE baseline.baseflow_batch_stat SET is_latest = false WHERE data_type = 'gantry';
    INSERT INTO baseline.baseflow_batch_stat (batch_id, data_type, start_time, end_time, data_count, is_latest, remark)
    VALUES (v_batch_id, 'gantry', v_start_ts, v_end_ts, v_gantry_count, true, '自动生成');
    -- 入口
    UPDATE baseline.baseflow_batch_stat SET is_latest = false WHERE data_type = 'tollsquare_on';
    INSERT INTO baseline.baseflow_batch_stat (batch_id, data_type, start_time, end_time, data_count, is_latest, remark)
    VALUES (v_batch_id, 'tollsquare_on', v_start_ts, v_end_ts, v_on_count, true, '自动生成');
    -- 出口
    UPDATE baseline.baseflow_batch_stat SET is_latest = false WHERE data_type = 'tollsquare_off';
    INSERT INTO baseline.baseflow_batch_stat (batch_id, data_type, start_time, end_time, data_count, is_latest, remark)
    VALUES (v_batch_id, 'tollsquare_off', v_start_ts, v_end_ts, v_off_count, true, '自动生成');
    -- OD
    UPDATE baseline.baseflow_batch_stat SET is_latest = false WHERE data_type = 'od';
    INSERT INTO baseline.baseflow_batch_stat (batch_id, data_type, start_time, end_time, data_count, is_latest, remark)
    VALUES (v_batch_id, 'od', v_start_ts, v_end_ts, v_od_count, true, '自动生成');
END;
$$ LANGUAGE plpgsql;

-- 使用示例：
-- SELECT user_generate_latest_4weeks_baseflow(); 