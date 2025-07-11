-- Step 1: 创建 baseline schema
CREATE SCHEMA IF NOT EXISTS baseline;

-- 1. 门架流量基准表
CREATE TABLE IF NOT EXISTS baseline.baseflow_pattern_gantry (
    gantry_id VARCHAR(50) NOT NULL,
    pattern_type VARCHAR(50) NOT NULL, -- workday/weekend/holiday_free/holiday_nofree
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    batch_id VARCHAR(20) NOT NULL, -- yyyymmdd_yyyymmdd
    mean NUMERIC(10,2) NOT NULL,
    stddev NUMERIC(10,2),
    median NUMERIC(10,2),
    p75 NUMERIC(10,2),
    p85 NUMERIC(10,2),
    data_points_count INTEGER,
    -- 六大类车型流量字段
    passenger_small NUMERIC(12,2),  -- k1+k2
    passenger_large NUMERIC(12,2),  -- k3+k4
    truck_small NUMERIC(12,2),      -- h1+h2
    truck_large NUMERIC(12,2),      -- h3+h4+h5+h6
    special_small NUMERIC(12,2),    -- t1+t2
    special_large NUMERIC(12,2),    -- t3+t4+t5+t6
    truck_ratio NUMERIC(6,4), -- 货车占比（h*+t*车型流量/总流量）
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (gantry_id, pattern_type, hour, batch_id)
);
CREATE INDEX idx_gantry_batch ON baseline.baseflow_pattern_gantry(batch_id);
COMMENT ON TABLE baseline.baseflow_pattern_gantry IS '门架流量基准表';

-- 2. 收费广场入口流量基准表
CREATE TABLE IF NOT EXISTS baseline.baseflow_pattern_tollsquare_on (
    square_code VARCHAR(50) NOT NULL,
    pattern_type VARCHAR(50) NOT NULL,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    batch_id VARCHAR(20) NOT NULL,
    mean NUMERIC(10,2) NOT NULL,
    stddev NUMERIC(10,2),
    median NUMERIC(10,2),
    p75 NUMERIC(10,2),
    p85 NUMERIC(10,2),
    data_points_count INTEGER,
    passenger_small NUMERIC(12,2),
    passenger_large NUMERIC(12,2),
    truck_small NUMERIC(12,2),
    truck_large NUMERIC(12,2),
    special_small NUMERIC(12,2),
    special_large NUMERIC(12,2),
    truck_ratio NUMERIC(6,4),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (square_code, pattern_type, hour, batch_id)
);
CREATE INDEX idx_tollsquare_on_batch ON baseline.baseflow_pattern_tollsquare_on(batch_id);
COMMENT ON TABLE baseline.baseflow_pattern_tollsquare_on IS '收费广场入口流量基准表';

-- 3. 收费广场出口流量基准表
CREATE TABLE IF NOT EXISTS baseline.baseflow_pattern_tollsquare_off (
    square_code VARCHAR(50) NOT NULL,
    pattern_type VARCHAR(50) NOT NULL,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    batch_id VARCHAR(20) NOT NULL,
    mean NUMERIC(10,2) NOT NULL,
    stddev NUMERIC(10,2),
    median NUMERIC(10,2),
    p75 NUMERIC(10,2),
    p85 NUMERIC(10,2),
    data_points_count INTEGER,
    passenger_small NUMERIC(12,2),
    passenger_large NUMERIC(12,2),
    truck_small NUMERIC(12,2),
    truck_large NUMERIC(12,2),
    special_small NUMERIC(12,2),
    special_large NUMERIC(12,2),
    truck_ratio NUMERIC(6,4),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (square_code, pattern_type, hour, batch_id)
);
CREATE INDEX idx_tollsquare_off_batch ON baseline.baseflow_pattern_tollsquare_off(batch_id);
COMMENT ON TABLE baseline.baseflow_pattern_tollsquare_off IS '收费广场出口流量基准表';

-- 4. OD基准流量表
CREATE TABLE IF NOT EXISTS baseline.baseflow_pattern_od (
    origin_id VARCHAR(100) NOT NULL, -- tollsquare_code或station_code
    destination_id VARCHAR(100) NOT NULL, -- tollsquare_code或station_code
    origin_type VARCHAR(20) NOT NULL, -- tollsquare/gantry
    destination_type VARCHAR(20) NOT NULL, -- tollsquare/gantry
    od_type_combination VARCHAR(30) NOT NULL, -- tollsquare-tollsquare等
    pattern_type VARCHAR(50) NOT NULL, -- workday/weekend/holiday_free/holiday_nofree
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    batch_id VARCHAR(20) NOT NULL,
    mean NUMERIC(10,2) NOT NULL,
    stddev NUMERIC(10,2),
    median NUMERIC(10,2),
    p75 NUMERIC(10,2),
    p85 NUMERIC(10,2),
    data_points_count INTEGER,
    avg_travel_time NUMERIC(10,2),
    passenger_small NUMERIC(12,2),
    passenger_large NUMERIC(12,2),
    truck_small NUMERIC(12,2),
    truck_large NUMERIC(12,2),
    special_small NUMERIC(12,2),
    special_large NUMERIC(12,2),
    truck_ratio NUMERIC(6,4),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (origin_id, destination_id, pattern_type, hour, batch_id)
);
CREATE INDEX idx_od_batch ON baseline.baseflow_pattern_od(batch_id);
COMMENT ON TABLE baseline.baseflow_pattern_od IS 'OD基准流量表，origin_id和destination_id为tollsquare_code，若为null则用station_code（门架）';

-- 5. 算法参数配置表
CREATE TABLE IF NOT EXISTS baseline.algorithm_config (
    config_id VARCHAR(100) PRIMARY KEY,
    config_group VARCHAR(50) NOT NULL,
    config_name VARCHAR(100) NOT NULL,
    config_value TEXT NOT NULL,
    value_type VARCHAR(20) NOT NULL, -- int/decimal/text/boolean
    description TEXT,
    valid_range TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE baseline.algorithm_config IS '算法参数配置表';

-- 6. 数据质量日志表
CREATE TABLE IF NOT EXISTS baseline.data_quality_log (
    log_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL, -- completeness/consistency/accuracy/timeliness
    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    issue_count INTEGER,
    issue_detail TEXT,
    resolved BOOLEAN DEFAULT FALSE
);
COMMENT ON TABLE baseline.data_quality_log IS '数据质量日志表';

-- 7. 执行日志表
CREATE TABLE IF NOT EXISTS baseline.execution_log (
    log_id BIGSERIAL PRIMARY KEY,
    execution_id VARCHAR(100) NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    log_level VARCHAR(20) NOT NULL, -- INFO/WARN/ERROR
    log_message TEXT NOT NULL,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_duration INTERVAL,
    records_affected INTEGER,
    additional_info JSONB
);
COMMENT ON TABLE baseline.execution_log IS '基准计算执行日志表'; 