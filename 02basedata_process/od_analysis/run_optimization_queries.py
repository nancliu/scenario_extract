#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
执行OD表优化查询的脚本
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

def run_optimization_queries():
    """执行优化查询"""
    
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
        print("OD表优化查询执行")
        print("=" * 80)
        
        # 1. 检查索引使用情况
        print("\n1. 索引使用统计:")
        index_usage_sql = """
        SELECT 
            indexrelname as index_name,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            CASE 
                WHEN seq_scan > 0 THEN idx_scan::float / seq_scan 
                ELSE idx_scan::float 
            END as index_vs_seq_ratio
        FROM pg_stat_user_indexes 
        WHERE schemaname = 'dwd' 
          AND relname = 'dwd_od_weekly'
        ORDER BY idx_scan DESC;
        """
        
        try:
            index_stats = pd.read_sql(index_usage_sql, engine)
            if not index_stats.empty:
                print(index_stats.to_string(index=False))
            else:
                print("   未找到索引使用统计")
        except Exception as e:
            print(f"   查询索引统计失败: {e}")
        
        # 2. 检查表统计信息
        print("\n2. 表统计信息:")
        table_stats_sql = """
        SELECT 
            schemaname,
            tablename,
            last_analyze,
            last_autoanalyze,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples
        FROM pg_stat_user_tables 
        WHERE schemaname = 'dwd' 
          AND relname = 'dwd_od_weekly';
        """
        
        try:
            table_stats = pd.read_sql(table_stats_sql, engine)
            if not table_stats.empty:
                print(table_stats.to_string(index=False))
            else:
                print("   未找到表统计信息")
        except Exception as e:
            print(f"   查询表统计失败: {e}")
        
        # 3. 检查分区信息 (PostgreSQL 10+)
        print("\n3. 分区信息:")
        partition_sql = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            pg_get_expr(c.relpartbound, c.oid) as partition_expression,
            pg_size_pretty(pg_total_relation_size(c.oid)) as size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relispartition 
          AND n.nspname = 'dwd'
          AND c.relname LIKE 'dwd_od_weekly%'
        ORDER BY c.relname;
        """
        
        try:
            partitions = pd.read_sql(partition_sql, engine)
            if not partitions.empty:
                print(partitions.to_string(index=False))
            else:
                print("   未找到分区信息")
        except Exception as e:
            print(f"   查询分区信息失败: {e}")
        
        # 4. 执行计划分析 - COUNT查询
        print("\n4. COUNT查询执行计划:")
        explain_count_sql = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) 
        SELECT COUNT(*) 
        FROM dwd.dwd_od_weekly 
        WHERE start_time >= '2025-07-07' 
          AND start_time < '2025-07-13';
        """
        
        try:
            explain_result = pd.read_sql(explain_count_sql, engine)
            for _, row in explain_result.iterrows():
                print(f"   {row.iloc[0]}")
        except Exception as e:
            print(f"   执行计划查询失败: {e}")
        
        # 5. 执行计划分析 - SELECT查询
        print("\n5. SELECT查询执行计划:")
        explain_select_sql = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) 
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
        """
        
        try:
            explain_result = pd.read_sql(explain_select_sql, engine)
            for _, row in explain_result.iterrows():
                print(f"   {row.iloc[0]}")
        except Exception as e:
            print(f"   执行计划查询失败: {e}")
        
        # 6. 检查现有索引详情
        print("\n6. 现有索引详情:")
        index_details_sql = """
        SELECT 
            i.relname as index_name,
            a.attname as column_name,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary,
            am.amname as index_type,
            pg_size_pretty(pg_relation_size(i.oid)) as index_size
        FROM pg_class t
        JOIN pg_namespace n ON t.relnamespace = n.oid
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = 'dwd' 
          AND t.relname = 'dwd_od_weekly'
        ORDER BY i.relname, a.attnum;
        """
        
        try:
            index_details = pd.read_sql(index_details_sql, engine)
            if not index_details.empty:
                print(index_details.to_string(index=False))
            else:
                print("   未找到索引详情")
        except Exception as e:
            print(f"   查询索引详情失败: {e}")
        
        # 7. 优化建议
        print("\n7. 优化建议:")
        print("   基于查询分析，建议考虑以下索引:")
        print("   ")
        print("   -- 如果经常按车辆类型过滤:")
        print("   CREATE INDEX CONCURRENTLY idx_dwd_od_weekly_time_vehicle")
        print("   ON dwd.dwd_od_weekly (start_time, vehicle_type);")
        print("   ")
        print("   -- 如果经常查询OD对:")
        print("   CREATE INDEX CONCURRENTLY idx_dwd_od_weekly_od_codes")
        print("   ON dwd.dwd_od_weekly (start_square_code, end_square_code, start_time);")
        print("   ")
        print("   -- 如果只关注最近数据:")
        print("   CREATE INDEX CONCURRENTLY idx_dwd_od_weekly_recent")
        print("   ON dwd.dwd_od_weekly (start_time)")
        print("   WHERE start_time >= CURRENT_DATE - INTERVAL '30 days';")
        print("   ")
        print("   -- 更新表统计信息:")
        print("   ANALYZE dwd.dwd_od_weekly;")
        
    except Exception as e:
        print(f"执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 设置环境变量
    os.environ['DB_HOST'] = '10.149.235.123'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'sdzg'
    os.environ['DB_USER'] = 'ln'
    os.environ['DB_PASSWORD'] = 'caneln'
    
    run_optimization_queries()
