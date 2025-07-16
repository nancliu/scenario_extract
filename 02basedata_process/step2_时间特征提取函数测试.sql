-- Step 2: user_extract_time_features 测试用例

-- 1. 正常工作日（如2025-01-15，周三，非节假日）
SELECT * FROM user_extract_time_features('2025-01-15 08:00:00'); -- 预期pattern_type=workday, is_workday=true

-- 2. 正常周末（如2025-01-18，周六，非节假日）
SELECT * FROM user_extract_time_features('2025-01-18 08:00:00'); -- 预期pattern_type=weekend, is_workday=false

-- 3. 免费节假日（如2025-02-10，春节，免费）
SELECT * FROM user_extract_time_features('2025-02-10 08:00:00'); -- 预期pattern_type=holiday_free, is_free_highway=true

-- 4. 收费节假日（如2025-06-01，端午节，收费）
SELECT * FROM user_extract_time_features('2025-06-01 08:00:00'); -- 预期pattern_type=holiday_nofree, is_free_highway=false

-- 5. 调休工作日（如2025-01-26，周日，调休上班）
SELECT * FROM user_extract_time_features('2025-01-26 08:00:00'); -- 预期pattern_type=workday, is_workday=true, is_adjusted_workday=true

-- 6. 调休取消的周末（如2025-01-26，周日，调休上班）
-- 同上，pattern_type=workday, is_workday=true, is_adjusted_workday=true 