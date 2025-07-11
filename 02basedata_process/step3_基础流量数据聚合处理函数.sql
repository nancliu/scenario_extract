-- Step 3: 门架流量基准初始化聚合函数
-- 依赖：user_extract_time_features

CREATE OR REPLACE FUNCTION user_generate_init_gantry_baseflow(p_batch_id VARCHAR DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    -- 获取dwd.dwd_flow_gantry表的时间范围
    SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_flow_gantry;
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
        f.gantry_id,
        t.pattern_type,
        t.hour_of_day,
        v_batch_id AS batch_id,
        AVG(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS mean,
        STDDEV_POP(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p85,
        COUNT(*) AS data_points_count,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(
            COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
            COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
            COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) = 0 THEN 0 ELSE
            SUM(
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            )::NUMERIC / NULLIF(SUM(
                COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            ),0)
        END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_flow_gantry f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.gantry_id, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 调用脚本示例：
-- SELECT user_generate_init_gantry_baseflow(); -- 自动按全量时间生成batch_id
-- SELECT user_generate_init_gantry_baseflow('20240601_20240630'); -- 指定批次号 

-- 收费广场入口流量基准初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_init_tollsquare_on_baseflow(p_batch_id VARCHAR DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_flow_onramp;
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
        AVG(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS mean,
        STDDEV_POP(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p85,
        COUNT(*) AS data_points_count,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(
            COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
            COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
            COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) = 0 THEN 0 ELSE
            SUM(
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            )::NUMERIC / NULLIF(SUM(
                COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            ),0)
        END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_flow_onramp f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.square_code, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 调用脚本示例：
-- SELECT user_generate_init_tollsquare_on_baseflow();
-- SELECT user_generate_init_tollsquare_on_baseflow('20240601_20240630');

-- 收费广场出口流量基准初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_init_tollsquare_off_baseflow(p_batch_id VARCHAR DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_flow_offramp;
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
        AVG(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS mean,
        STDDEV_POP(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p85,
        COUNT(*) AS data_points_count,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(
            COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
            COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
            COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) = 0 THEN 0 ELSE
            SUM(
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            )::NUMERIC / NULLIF(SUM(
                COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            ),0)
        END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_flow_offramp f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.square_code, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 调用脚本示例：
-- SELECT user_generate_init_tollsquare_off_baseflow();
-- SELECT user_generate_init_tollsquare_off_baseflow('20240601_20240630');

-- OD基准流量初始化聚合函数
CREATE OR REPLACE FUNCTION user_generate_init_od_baseflow(p_batch_id VARCHAR DEFAULT NULL)
RETURNS VOID AS $$
DECLARE
    v_min_time TIMESTAMP;
    v_max_time TIMESTAMP;
    v_batch_id VARCHAR;
BEGIN
    SELECT MIN(start_time), MAX(start_time) INTO v_min_time, v_max_time FROM dwd.dwd_od_g4202;
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
        f.start_square_code AS origin_id,
        f.end_square_code AS destination_id,
        'tollsquare' AS origin_type,
        'tollsquare' AS destination_type,
        'tollsquare-tollsquare' AS od_type_combination,
        t.pattern_type,
        t.hour_of_day,
        v_batch_id AS batch_id,
        AVG(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS mean,
        STDDEV_POP(
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS stddev,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p75,
        PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY 
          COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
          COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
          COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) AS p85,
        COUNT(*) AS data_points_count,
        AVG(f.avg_travel_time) AS avg_travel_time,
        AVG(COALESCE(f.k1,0) + COALESCE(f.k2,0)) AS passenger_small,
        AVG(COALESCE(f.k3,0) + COALESCE(f.k4,0)) AS passenger_large,
        AVG(COALESCE(f.h1,0) + COALESCE(f.h2,0)) AS truck_small,
        AVG(COALESCE(f.h3,0) + COALESCE(f.h4,0) + COALESCE(f.h5,0) + COALESCE(f.h6,0)) AS truck_large,
        AVG(COALESCE(f.t1,0) + COALESCE(f.t2,0)) AS special_small,
        AVG(COALESCE(f.t3,0) + COALESCE(f.t4,0) + COALESCE(f.t5,0) + COALESCE(f.t6,0)) AS special_large,
        CASE WHEN SUM(
            COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
            COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
            COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
        ) = 0 THEN 0 ELSE
            SUM(
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            )::NUMERIC / NULLIF(SUM(
                COALESCE(f.k1,0)+COALESCE(f.k2,0)+COALESCE(f.k3,0)+COALESCE(f.k4,0)+
                COALESCE(f.h1,0)+COALESCE(f.h2,0)+COALESCE(f.h3,0)+COALESCE(f.h4,0)+COALESCE(f.h5,0)+COALESCE(f.h6,0)+
                COALESCE(f.t1,0)+COALESCE(f.t2,0)+COALESCE(f.t3,0)+COALESCE(f.t4,0)+COALESCE(f.t5,0)+COALESCE(f.t6,0)
            ),0)
        END AS truck_ratio,
        NOW() AS update_time
    FROM dwd.dwd_od_g4202 f
    JOIN LATERAL user_extract_time_features(f.start_time) t ON TRUE
    WHERE f.start_time >= v_min_time AND f.start_time <= v_max_time
    GROUP BY f.start_square_code, f.end_square_code, t.pattern_type, t.hour_of_day;
END;
$$ LANGUAGE plpgsql;

-- 调用脚本示例：
-- SELECT user_generate_init_od_baseflow();
-- SELECT user_generate_init_od_baseflow('20240601_20240630'); 