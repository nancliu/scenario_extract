#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试详细关联分析脚本的功能
使用模拟数据验证脚本功能是否正常
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from detailed_correlation_analysis import DetailedCorrelationAnalyzer

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockDetailedCorrelationAnalyzer(DetailedCorrelationAnalyzer):
    """使用模拟数据的测试分析器"""
    
    def __init__(self):
        """初始化测试分析器"""
        self.db_config = {}
        self.engine = None
        self.od_data = None
        self.gantry_flow_data = None
        self.onramp_flow_data = None
        
    def connect_database(self):
        """模拟数据库连接"""
        logger.info("使用模拟数据，跳过数据库连接")
        
    def load_data(self, start_date: str, end_date: str, sample_size: int = None):
        """生成模拟数据"""
        logger.info(f"生成模拟数据: {start_date} 到 {end_date}")
        
        # 生成模拟OD数据
        np.random.seed(42)  # 固定随机种子以便重现
        
        # 生成日期范围
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        date_range = pd.date_range(start_dt, end_dt, freq='H')
        
        # 生成门架和收费广场编码
        gantry_codes = [f'G{i:04d}' for i in range(1, 51)]  # 50个门架
        square_codes = [f'S{i:04d}' for i in range(1, 31)]  # 30个收费广场
        
        # 生成OD数据
        n_records = sample_size if sample_size else 10000
        od_records = []
        
        for i in range(n_records):
            # 随机选择起点类型
            start_type = np.random.choice(['gantry', 'square'], p=[0.6, 0.4])
            
            if start_type == 'gantry':
                start_square_code = None
                start_square_name = None
                start_station_code = np.random.choice(gantry_codes)
                start_station_name = f'门架{start_station_code}'
            else:
                start_square_code = np.random.choice(square_codes)
                start_square_name = f'收费广场{start_square_code}'
                start_station_code = start_square_code
                start_station_name = start_square_name
            
            # 随机选择终点
            end_type = np.random.choice(['gantry', 'square'], p=[0.6, 0.4])
            if end_type == 'gantry':
                end_square_code = None
                end_square_name = None
                end_station_code = np.random.choice(gantry_codes)
                end_station_name = f'门架{end_station_code}'
            else:
                end_square_code = np.random.choice(square_codes)
                end_square_name = f'收费广场{end_square_code}'
                end_station_code = end_square_code
                end_station_name = end_square_name
            
            # 确保起点和终点不同
            while start_station_code == end_station_code:
                if end_type == 'gantry':
                    end_station_code = np.random.choice(gantry_codes)
                    end_station_name = f'门架{end_station_code}'
                else:
                    end_square_code = np.random.choice(square_codes)
                    end_square_name = f'收费广场{end_square_code}'
                    end_station_code = end_square_code
                    end_station_name = end_square_name
            
            # 随机时间
            start_time = np.random.choice(date_range)
            end_time = start_time + timedelta(hours=np.random.randint(1, 6))
            
            # 车辆类型
            vehicle_type = np.random.choice(['k1', 'k2', 'h1', 'h2', 'h3'], p=[0.6, 0.2, 0.1, 0.05, 0.05])
            
            od_records.append({
                'pass_id': f'P{i:08d}',
                'vehicle_type': vehicle_type,
                'start_time': start_time,
                'start_square_code': start_square_code,
                'start_square_name': start_square_name,
                'start_station_code': start_station_code,
                'start_station_name': start_station_name,
                'end_time': end_time,
                'end_square_code': end_square_code,
                'end_square_name': end_square_name,
                'end_station_code': end_station_code,
                'end_station_name': end_station_name,
                'direction': np.random.choice(['上行', '下行'])
            })
        
        self.od_data = pd.DataFrame(od_records)
        logger.info(f"生成OD数据: {len(self.od_data)} 条记录")
        
        # 处理OD数据
        self._process_od_data()
        
        # 生成门架流量数据
        gantry_flow_records = []
        for gantry_code in gantry_codes:
            for dt in date_range:
                # 生成流量数据
                base_flow = np.random.randint(50, 500)
                k_flow = int(base_flow * np.random.uniform(0.6, 0.8))
                h_flow = int(base_flow * np.random.uniform(0.15, 0.35))
                t_flow = base_flow - k_flow - h_flow
                
                gantry_flow_records.append({
                    'start_gantryid': gantry_code,
                    'start_time': dt,
                    'total': base_flow,
                    'total_k': k_flow,
                    'total_h': h_flow,
                    'total_t': t_flow
                })
        
        self.gantry_flow_data = pd.DataFrame(gantry_flow_records)
        logger.info(f"生成门架流量数据: {len(self.gantry_flow_data)} 条记录")
        
        # 生成收费广场流量数据
        square_flow_records = []
        for square_code in square_codes:
            for dt in date_range:
                # 生成流量数据
                base_flow = np.random.randint(30, 300)
                k_flow = int(base_flow * np.random.uniform(0.6, 0.8))
                h_flow = int(base_flow * np.random.uniform(0.15, 0.35))
                t_flow = base_flow - k_flow - h_flow
                
                square_flow_records.append({
                    'square_code': square_code,
                    'start_time': dt,
                    'total': base_flow,
                    'total_k': k_flow,
                    'total_h': h_flow,
                    'total_t': t_flow
                })
        
        self.onramp_flow_data = pd.DataFrame(square_flow_records)
        logger.info(f"生成收费广场流量数据: {len(self.onramp_flow_data)} 条记录")

def main():
    """主函数"""
    print("=" * 80)
    print("详细起始点关联分析功能测试")
    print("=" * 80)
    print("使用模拟数据测试脚本功能")
    print("=" * 80)
    
    try:
        # 创建测试分析器
        analyzer = MockDetailedCorrelationAnalyzer()
        
        # 连接数据库（模拟）
        analyzer.connect_database()
        
        # 加载数据（模拟）
        print("1. 生成模拟数据...")
        analyzer.load_data('2025-07-07', '2025-07-14', 5000)
        
        # 门架关联分析
        print("2. 执行门架关联分析...")
        gantry_results = analyzer.analyze_gantry_correlation_detailed()
        
        if gantry_results:
            print(f"   门架关联分析完成，关联记录数: {gantry_results.get('total_records', 0)}")
            print(f"   相关系数: {gantry_results.get('correlation_coefficient', 0):.3f}")
        else:
            print("   门架关联分析未返回结果")
        
        # 收费广场关联分析
        print("3. 执行收费广场关联分析...")
        square_results = analyzer.analyze_toll_square_correlation_detailed()
        
        if square_results:
            print(f"   收费广场关联分析完成，关联记录数: {square_results.get('total_records', 0)}")
            print(f"   相关系数: {square_results.get('correlation_coefficient', 0):.3f}")
        else:
            print("   收费广场关联分析未返回结果")
        
        # 生成报告
        print("4. 生成分析报告...")
        output_dir = 'test_correlation_output'
        os.makedirs(output_dir, exist_ok=True)
        
        report_file = os.path.join(output_dir, 'test_correlation_report.html')
        analyzer.generate_report(gantry_results, square_results, report_file)
        
        print(f"\n✅ 测试完成！")
        print(f"📁 结果保存在: {output_dir}")
        print(f"📄 查看报告: {report_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
