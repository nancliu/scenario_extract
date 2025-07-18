#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试数据结构脚本
检查OD数据和流量数据的字段结构
"""

import os
import sys
import pandas as pd
import logging

# 添加配置路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from config import get_config, validate_config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """主函数"""
    print("=" * 60)
    print("数据结构调试脚本")
    print("=" * 60)
    
    try:
        # 验证配置
        if not validate_config():
            return 1
            
        # 获取配置
        config = get_config()
        db_config = config['database']
        
        # 创建数据库连接
        from sqlalchemy import create_engine
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        
        print("1. 检查OD数据结构...")
        od_sql = """
        SELECT *
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '2025-07-07 08:00:00'
          AND start_time < '2025-07-07 08:05:00'
        LIMIT 5
        """
        
        od_sample = pd.read_sql(od_sql, engine)
        print(f"OD数据样本 ({len(od_sample)} 条记录):")
        print("字段列表:", list(od_sample.columns))
        print("\n样本数据:")
        print(od_sample[['start_square_code', 'start_station_code', 'end_square_code', 'end_station_code']].head())
        
        print("\n2. 检查门架流量数据结构...")
        gantry_sql = """
        SELECT *
        FROM dwd.dwd_flow_gantry_weekly
        WHERE start_time >= '2025-07-07 08:00:00'
          AND start_time < '2025-07-07 08:05:00'
        LIMIT 5
        """
        
        gantry_sample = pd.read_sql(gantry_sql, engine)
        print(f"门架流量数据样本 ({len(gantry_sample)} 条记录):")
        print("字段列表:", list(gantry_sample.columns))
        print("\n样本数据:")
        print(gantry_sample[['start_gantryid', 'gantry_name', 'total']].head())
        
        print("\n3. 检查收费广场入口流量数据结构...")
        onramp_sql = """
        SELECT *
        FROM dwd.dwd_flow_onramp_weekly
        WHERE start_time >= '2025-07-07 08:00:00'
          AND start_time < '2025-07-07 08:05:00'
        LIMIT 5
        """
        
        onramp_sample = pd.read_sql(onramp_sql, engine)
        print(f"收费广场入口流量数据样本 ({len(onramp_sample)} 条记录):")
        print("字段列表:", list(onramp_sample.columns))
        print("\n样本数据:")
        print(onramp_sample[['square_code', 'total']].head())
        
        print("\n4. 检查收费广场出口流量数据结构...")
        offramp_sql = """
        SELECT *
        FROM dwd.dwd_flow_offramp_weekly
        WHERE start_time >= '2025-07-07 08:00:00'
          AND start_time < '2025-07-07 08:05:00'
        LIMIT 5
        """
        
        offramp_sample = pd.read_sql(offramp_sql, engine)
        print(f"收费广场出口流量数据样本 ({len(offramp_sample)} 条记录):")
        print("字段列表:", list(offramp_sample.columns))
        print("\n样本数据:")
        print(offramp_sample[['square_code', 'total']].head())
        
        print("\n5. 分析OD数据中的门架和收费广场分布...")
        
        # 统计起点类型
        od_start_gantry = od_sample[pd.isna(od_sample['start_square_code'])]
        od_start_square = od_sample[pd.notna(od_sample['start_square_code'])]
        
        print(f"起点为门架的记录: {len(od_start_gantry)} 条")
        print(f"起点为收费广场的记录: {len(od_start_square)} 条")
        
        if len(od_start_gantry) > 0:
            print("门架起点示例:", od_start_gantry['start_station_code'].head().tolist())
        
        if len(od_start_square) > 0:
            print("收费广场起点示例:", od_start_square['start_square_code'].head().tolist())
        
        # 统计终点类型
        od_end_gantry = od_sample[pd.isna(od_sample['end_square_code'])]
        od_end_square = od_sample[pd.notna(od_sample['end_square_code'])]
        
        print(f"终点为门架的记录: {len(od_end_gantry)} 条")
        print(f"终点为收费广场的记录: {len(od_end_square)} 条")
        
        print("\n✅ 数据结构检查完成！")
        return 0
        
    except Exception as e:
        logger.error(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
