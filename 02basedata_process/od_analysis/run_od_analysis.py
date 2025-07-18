#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OD数据分析执行脚本

快速执行OD数据分析的简化脚本
"""

import sys
import os
from datetime import datetime, timedelta
import argparse

# 添加当前目录和config目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
config_dir = os.path.join(parent_dir, 'config')

sys.path.append(current_dir)
sys.path.append(config_dir)

from od_data_analysis import ODDataAnalyzer
from config import get_config, validate_config

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='OD数据分析工具')
    
    parser.add_argument(
        '--start-date', 
        type=str, 
        help='开始日期 (YYYY-MM-DD)，默认为7天前'
    )
    
    parser.add_argument(
        '--end-date', 
        type=str, 
        help='结束日期 (YYYY-MM-DD)，默认为今天'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100000,  # 默认限制10万条记录
        help='限制记录数，默认100000（用于避免大数据集性能问题）'
    )
    
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default='od_analysis_output',
        help='输出目录，默认为 od_analysis_output'
    )
    
    parser.add_argument(
        '--config-check', 
        action='store_true',
        help='仅检查配置，不执行分析'
    )
    
    parser.add_argument(
        '--quick',
        action='store_true',
        help='快速模式，限制数据量为10000条'
    )

    parser.add_argument(
        '--no-limit',
        action='store_true',
        help='移除记录数限制（注意：大数据集可能很慢）'
    )
    
    return parser.parse_args()

def main():
    """主函数"""
    print("=" * 60)
    print("OD数据分析工具")
    print("=" * 60)
    
    # 解析参数
    args = parse_arguments()
    
    # 验证配置
    print("1. 验证配置...")
    if not validate_config():
        print("❌ 配置验证失败，请检查config.py中的数据库配置")
        return 1
    print("✅ 配置验证通过")
    
    # 如果只是检查配置，则退出
    if args.config_check:
        print("配置检查完成")
        return 0
    
    # 获取配置
    config = get_config()
    
    # 设置日期范围
    if args.end_date:
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # 设置限制
    if args.quick:
        limit = 10000
    elif args.no_limit:
        limit = None
    else:
        limit = args.limit
    
    print(f"2. 分析参数:")
    print(f"   开始日期: {start_date}")
    print(f"   结束日期: {end_date}")
    print(f"   记录限制: {limit if limit else '无限制'}")
    print(f"   输出目录: {args.output_dir}")
    
    try:
        # 创建分析器
        print("\n3. 初始化分析器...")
        analyzer = ODDataAnalyzer(config['database'])
        
        # 执行分析
        print("4. 开始数据分析...")
        results = analyzer.run_full_analysis(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            output_dir=args.output_dir
        )
        
        # 显示结果摘要
        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)
        print(f"📊 总记录数: {results['stats']['total_records']:,}")
        print(f"🔗 唯一OD对: {results['stats']['unique_od_pairs']:,}")
        print(f"🚗 车辆类型数: {len(results['stats']['vehicle_type_dist'])}")
        print(f"🏢 起点收费站: {results['stats']['unique_start_stations']}")
        print(f"🏁 终点收费站: {results['stats']['unique_end_stations']}")
        
        # 异常情况摘要
        anomaly_count = sum([
            v if isinstance(v, int) else len(v) if isinstance(v, dict) else 0 
            for v in results['anomalies'].values()
        ])
        
        if anomaly_count > 0:
            print(f"⚠️  检测到异常: {anomaly_count} 项")
        else:
            print("✅ 数据质量良好")
        
        print(f"\n📁 结果保存在: {results['output_dir']}")
        print(f"📄 查看报告: {os.path.join(results['output_dir'], 'od_analysis_report.html')}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n用户中断分析")
        return 1
        
    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
