#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OD数据查询性能测试脚本
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text

def test_od_query_performance():
    """测试OD查询性能"""
    
    # 数据库配置
    db_config = {
        'host': os.getenv('DB_HOST', '10.149.235.123'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'sdzg'),
        'user': os.getenv('DB_USER', 'ln'),
        'password': os.getenv('DB_PASSWORD', 'caneln')
    }
    
    try:
        # 连接数据库
        connection_string = (
            f"postgresql://{db_config['user']}:"
            f"{db_config['password']}@{db_config['host']}:"
            f"{db_config['port']}/{db_config['database']}"
        )
        engine = create_engine(connection_string)
        
        print("=" * 80)
        print("OD数据查询性能测试")
        print("=" * 80)
        
        # 测试日期范围
        start_date = '2025-07-07'
        end_date = '2025-07-13'
        
        # 1. 测试记录数统计查询
        print(f"\n1. 统计查询测试 ({start_date} 到 {end_date})")
        count_sql = f"""
        SELECT COUNT(*) as total_count
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        """
        
        start_time = time.time()
        count_result = pd.read_sql(count_sql, engine)
        count_duration = time.time() - start_time
        total_count = count_result['total_count'].iloc[0]
        
        print(f"   记录数: {total_count:,}")
        print(f"   耗时: {count_duration:.2f} 秒")
        
        # 2. 测试EXPLAIN查询计划
        print(f"\n2. 查询计划分析")
        explain_sql = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) 
        SELECT pass_id, vehicle_type, start_time, start_square_code, end_square_code
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        LIMIT 1000
        """
        
        explain_result = pd.read_sql(explain_sql, engine)
        print("   执行计划:")
        for _, row in explain_result.iterrows():
            print(f"   {row.iloc[0]}")
        
        # 3. 测试小批量数据查询
        print(f"\n3. 小批量查询测试 (LIMIT 1000)")
        small_sql = f"""
        SELECT 
            pass_id,
            vehicle_type,
            start_time,
            start_square_code,
            start_square_name,
            start_station_code,
            start_station_name,
            end_square_code,
            end_square_name,
            end_station_code,
            end_station_name
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        LIMIT 1000
        """
        
        start_time = time.time()
        small_result = pd.read_sql(small_sql, engine)
        small_duration = time.time() - start_time
        
        print(f"   返回记录数: {len(small_result):,}")
        print(f"   耗时: {small_duration:.2f} 秒")
        
        # 4. 测试不同LIMIT大小的性能
        print(f"\n4. 不同数据量查询性能对比")
        limits = [1000, 5000, 10000, 50000]
        
        base_sql = f"""
        SELECT 
            pass_id,
            vehicle_type,
            start_time,
            start_square_code,
            end_square_code
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        LIMIT {{}}
        """
        
        for limit in limits:
            if limit > total_count:
                print(f"   LIMIT {limit:,}: 跳过 (超过总记录数)")
                continue
                
            start_time = time.time()
            result = pd.read_sql(base_sql.format(limit), engine)
            duration = time.time() - start_time
            
            rate = len(result) / duration if duration > 0 else 0
            print(f"   LIMIT {limit:,}: {duration:.2f}秒, {rate:.0f} 记录/秒")
        
        # 5. 测试分区查询性能
        print(f"\n5. 分区信息查询")
        partition_sql = """
        SELECT 
            schemaname,
            tablename,
            partitionboundary,
            partitiontablename
        FROM pg_partitions 
        WHERE schemaname = 'dwd' 
          AND tablename = 'dwd_od_weekly'
        ORDER BY partitionboundary;
        """
        
        try:
            partitions = pd.read_sql(partition_sql, engine)
            if not partitions.empty:
                print("   分区列表:")
                for _, row in partitions.iterrows():
                    print(f"   - {row['partitiontablename']}: {row['partitionboundary']}")
            else:
                print("   未找到分区信息")
        except Exception as e:
            print(f"   查询分区信息失败: {e}")
        
        # 6. 索引使用情况
        print(f"\n6. 索引使用统计")
        index_usage_sql = """
        SELECT 
            indexrelname as index_name,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched
        FROM pg_stat_user_indexes 
        WHERE schemaname = 'dwd' 
          AND relname = 'dwd_od_weekly'
        ORDER BY idx_scan DESC;
        """
        
        index_stats = pd.read_sql(index_usage_sql, engine)
        if not index_stats.empty:
            print(index_stats.to_string(index=False))
        else:
            print("   未找到索引使用统计")
        
        # 7. 性能优化建议
        print(f"\n7. 性能优化建议")
        
        if total_count > 1000000:
            print("   ⚠️  数据量较大，建议:")
            print("      - 使用LIMIT限制返回记录数")
            print("      - 考虑分批处理")
            print("      - 只选择必要的字段")
        
        if count_duration > 5:
            print("   ⚠️  统计查询较慢，可能需要:")
            print("      - 检查分区剪枝是否生效")
            print("      - 考虑创建部分索引")
        
        print(f"\n   建议的查询优化:")
        print(f"   - 移除ORDER BY子句（如果不需要排序）")
        print(f"   - 使用分页查询代替大批量查询")
        print(f"   - 考虑创建复合索引: (start_time, vehicle_type)")
        
    except Exception as e:
        print(f"性能测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 设置环境变量
    os.environ['DB_HOST'] = '10.149.235.123'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'sdzg'
    os.environ['DB_USER'] = 'ln'
    os.environ['DB_PASSWORD'] = 'caneln'
    
    test_od_query_performance()
