#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细的起始点关联分析脚本

分析OD数据起始点与对应流量数据的详细关联性，包括：
1. 门架起点与门架流量的关联分析
2. 收费广场起点与收费广场流量的关联分析
3. 详细的统计分析
4. 时间模式分析
5. 车型结构对比分析
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# 添加配置路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from config import get_config, validate_config

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DetailedCorrelationAnalyzer:
    """详细关联分析器"""

    def __init__(self, db_config=None):
        """初始化分析器"""
        if db_config is None:
            # 使用配置文件中的数据库配置
            config = get_config()
            self.db_config = config['database']
        else:
            self.db_config = db_config
        self.engine = None
        self.od_data = None
        self.gantry_flow_data = None
        self.onramp_flow_data = None
        self.offramp_flow_data = None
        
    def connect_database(self):
        """连接数据库"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:"
                f"{self.db_config['password']}@{self.db_config['host']}:"
                f"{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def _get_year_from_date(self, date_str: str) -> int:
        """从日期字符串中提取年份"""
        from datetime import datetime
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').year

    def _is_2024_data(self, start_date: str) -> bool:
        """判断是否为2024年数据"""
        return self._get_year_from_date(start_date) == 2024

    def _get_table_names(self, start_date: str) -> dict:
        """根据年份获取对应的表名"""
        if self._is_2024_data(start_date):
            return {
                'od_table': 'dwd.dwd_od_g4202',
                'gantry_table': 'dwd.dwd_flow_gantry', 
                'onramp_table': 'dwd.dwd_flow_onramp',
                'offramp_table': 'dwd.dwd_flow_offramp'
            }
        else:
            return {
                'od_table': 'dwd.dwd_od_weekly',
                'gantry_table': 'dwd.dwd_flow_gantry_weekly',
                'onramp_table': 'dwd.dwd_flow_onramp_weekly', 
                'offramp_table': 'dwd.dwd_flow_offramp_weekly'
            }

    def _build_od_sql(self, table_name: str, start_date: str, end_date: str, time_filter: str) -> str:
        """构建OD数据查询SQL"""
        return f"""
        SELECT
            pass_id,
            start_time,
            start_station_code,
            start_square_code,
            end_time,
            end_station_code,
            end_square_code,
            vehicle_type
        FROM {table_name}
        WHERE {time_filter} >= '{start_date}'
          AND {time_filter} < '{end_date}'
        ORDER BY start_time
        """

    def _build_gantry_flow_sql(self, table_name: str, start_date: str, end_date: str) -> str:
        """构建门架流量数据查询SQL"""
        if self._is_2024_data(start_date):
            return f"""
            SELECT
                gantry_id as station_code,
                start_time,
                (k1+k2+k3+k4+h1+h2+h3+h4+h5+h6+t1+t2+t3+t4+t5+t6) as total,
                (k1+k2+k3+k4) as total_k,
                (h1+h2+h3+h4+h5+h6) as total_h,
                (t1+t2+t3+t4+t5+t6) as total_t
            FROM {table_name}
            WHERE start_time >= '{start_date}'
              AND start_time < '{end_date}'
            """
        else:
            return f"""
            SELECT
                start_gantryid as station_code,
                start_time,
                total,
                total_k,
                total_h,
                total_t
            FROM {table_name}
            WHERE start_time >= '{start_date}'
              AND start_time < '{end_date}'
            """

    def _build_square_flow_sql(self, table_name: str, start_date: str, end_date: str) -> str:
        """构建收费广场流量数据查询SQL"""
        if self._is_2024_data(start_date):
            return f"""
            SELECT
                square_code,
                start_time,
                total,
                (k1+k2+k3+k4) as total_k,
                (h1+h2+h3+h4+h5+h6) as total_h,
                (t1+t2+t3+t4+t5+t6) as total_t
            FROM {table_name}
            WHERE start_time >= '{start_date}'
              AND start_time < '{end_date}'
            """
        else:
            return f"""
            SELECT
                square_code,
                start_time,
                total,
                total_k,
                total_h,
                total_t
            FROM {table_name}
            WHERE start_time >= '{start_date}'
              AND start_time < '{end_date}'
            """

    def load_data(self, start_date: str, end_date: str):
        """加载分析数据 - 支持2024和2025年数据"""
        logger.info(f"加载数据: {start_date} 到 {end_date}")
        
        # 获取对应年份的表名
        tables = self._get_table_names(start_date)
        year = self._get_year_from_date(start_date)
        logger.info(f"检测到{year}年数据，使用表: {list(tables.values())}")
        
        # 加载OD数据 - 分别加载入口和出口分析需要的数据
        od_sql_entry = self._build_od_sql(tables['od_table'], start_date, end_date, 'start_time')
        od_sql_exit = self._build_od_sql(tables['od_table'], start_date, end_date, 'end_time')
        
        # 分别加载入口和出口数据
        od_data_entry = pd.read_sql(od_sql_entry, self.engine)
        od_data_exit = pd.read_sql(od_sql_exit, self.engine)
        
        # 合并去重（使用pass_id作为唯一标识）
        self.od_data = pd.concat([od_data_entry, od_data_exit]).drop_duplicates(subset=['pass_id']).reset_index(drop=True)
        
        logger.info(f"入口分析OD数据: {len(od_data_entry):,} 条记录")
        logger.info(f"出口分析OD数据: {len(od_data_exit):,} 条记录")
        logger.info(f"合并后OD数据: {len(self.od_data):,} 条记录")

        # 数据预处理
        self._process_od_data()

        # 加载门架流量数据
        gantry_sql = self._build_gantry_flow_sql(tables['gantry_table'], start_date, end_date)
        self.gantry_flow_data = pd.read_sql(gantry_sql, self.engine)
        logger.info(f"门架流量数据加载完成: {len(self.gantry_flow_data):,} 条记录")

        # 加载收费广场入口流量数据
        onramp_sql = self._build_square_flow_sql(tables['onramp_table'], start_date, end_date)
        self.onramp_flow_data = pd.read_sql(onramp_sql, self.engine)
        logger.info(f"收费广场入口流量数据加载完成: {len(self.onramp_flow_data):,} 条记录")

        # 加载收费广场出口流量数据
        offramp_sql = self._build_square_flow_sql(tables['offramp_table'], start_date, end_date)
        self.offramp_flow_data = pd.read_sql(offramp_sql, self.engine)
        logger.info(f"收费广场出口流量数据加载完成: {len(self.offramp_flow_data):,} 条记录")
    
    def _process_od_data(self):
        """处理OD数据"""
        # 构建起点编码
        self.od_data['start_point_code'] = self.od_data['start_square_code'].fillna(self.od_data['start_station_code'])
        self.od_data['end_point_code'] = self.od_data['end_square_code'].fillna(self.od_data['end_station_code'])
        
        # 标记点位类型
        self.od_data['start_point_type'] = self.od_data['start_square_code'].apply(
            lambda x: 'toll_square' if pd.notna(x) else 'gantry'
        )
        self.od_data['end_point_type'] = self.od_data['end_square_code'].apply(
            lambda x: 'toll_square' if pd.notna(x) else 'gantry'
        )
        
        # 添加时间字段
        self.od_data['start_datetime'] = pd.to_datetime(self.od_data['start_time'])
        self.od_data['start_hour'] = self.od_data['start_datetime'].dt.hour
        self.od_data['start_date'] = self.od_data['start_datetime'].dt.date
        self.od_data['start_weekday'] = self.od_data['start_datetime'].dt.dayofweek

        self.od_data['end_datetime'] = pd.to_datetime(self.od_data['end_time'])
        self.od_data['end_hour'] = self.od_data['end_datetime'].dt.hour
        self.od_data['end_date'] = self.od_data['end_datetime'].dt.date
        self.od_data['end_weekday'] = self.od_data['end_datetime'].dt.dayofweek
        
        # 车型分类
        def classify_vehicle_type(vehicle_type):
            if vehicle_type.startswith('k'):
                return 'passenger'
            elif vehicle_type.startswith('h'):
                return 'truck'
            elif vehicle_type.startswith('t'):
                return 'trailer'
            else:
                return 'other'
        
        self.od_data['vehicle_class'] = self.od_data['vehicle_type'].apply(classify_vehicle_type)
        
        logger.info("OD数据处理完成")
    
    def analyze_gantry_correlation_detailed(self):
        """详细的门架关联分析"""
        logger.info("开始详细门架关联分析")
        
        # 筛选门架起点的OD数据
        gantry_od = self.od_data[self.od_data['start_point_type'] == 'gantry'].copy()
        
        if len(gantry_od) == 0:
            logger.warning("没有门架起点的OD数据")
            return {}
        
        logger.info(f"门架起点OD数据总数: {len(gantry_od)}")
        logger.info(f"门架流量数据总数: {len(self.gantry_flow_data)}")
        
        # 处理门架流量数据
        gantry_flow = self.gantry_flow_data.copy()
        gantry_flow['flow_datetime'] = pd.to_datetime(gantry_flow['start_time'])
        gantry_flow['flow_hour'] = gantry_flow['flow_datetime'].dt.hour
        gantry_flow['flow_date'] = gantry_flow['flow_datetime'].dt.date
        
        # 按门架、日期、小时聚合OD数据
        od_aggregated = gantry_od.groupby(['start_station_code', 'start_date', 'start_hour']).agg({
            'pass_id': 'count',
            'vehicle_class': lambda x: x.value_counts().to_dict()
        }).reset_index()
        od_aggregated.columns = ['station_code', 'od_date', 'od_hour', 'od_count', 'od_vehicle_dist']
        
        # 按门架、日期、小时聚合流量数据
        flow_aggregated = gantry_flow.groupby(['station_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum',
            'total_k': 'sum',
            'total_h': 'sum',
            'total_t': 'sum'
        }).reset_index()
        flow_aggregated.columns = ['station_code', 'flow_date', 'flow_hour', 'flow_total', 'flow_k', 'flow_h', 'flow_t']

        # 数据质量诊断
        od_stations = set(od_aggregated['station_code'].unique())
        flow_stations = set(flow_aggregated['station_code'].unique())
        common_stations = od_stations.intersection(flow_stations)
        od_only_stations = od_stations - flow_stations
        flow_only_stations = flow_stations - od_stations
        
        logger.info(f"OD数据涉及门架数: {len(od_stations)}")
        logger.info(f"流量数据涉及门架数: {len(flow_stations)}")
        logger.info(f"共同门架数: {len(common_stations)}")
        
        if od_only_stations:
            logger.warning(f"仅OD有的门架数: {len(od_only_stations)}, 示例: {list(od_only_stations)[:3]}")
        if flow_only_stations:
            logger.warning(f"仅流量有的门架数: {len(flow_only_stations)}, 示例: {list(flow_only_stations)[:3]}")

        # 使用left join保留所有OD数据
        correlation_data = pd.merge(
            od_aggregated,
            flow_aggregated,
            left_on=['station_code', 'od_date', 'od_hour'],
            right_on=['station_code', 'flow_date', 'flow_hour'],
            how='left'
        )
        
        if len(correlation_data) == 0:
            logger.warning("门架关联后没有数据")
            return {}

        # 数据质量评估
        missing_flow_count = correlation_data['flow_total'].isnull().sum()
        matched_records = len(correlation_data) - missing_flow_count
        
        logger.info(f"门架关联结果统计:")
        logger.info(f"  - 总记录数: {len(correlation_data)}")
        logger.info(f"  - 有流量匹配的记录数: {matched_records}")
        logger.info(f"  - 缺失流量的记录数: {missing_flow_count}")
        logger.info(f"  - OD总数: {correlation_data['od_count'].sum()}")
        
        if matched_records > 0:
            matched_od_count = correlation_data[correlation_data['flow_total'].notna()]['od_count'].sum()
            matched_flow_total = correlation_data['flow_total'].dropna().sum()
            logger.info(f"  - 匹配部分OD总数: {matched_od_count}")
            logger.info(f"  - 匹配部分流量总数: {matched_flow_total}")
            if matched_flow_total > 0:
                logger.info(f"  - 匹配部分OD/流量比: {matched_od_count / matched_flow_total:.3f}")

        # 计算详细统计
        stats = self._calculate_detailed_stats(correlation_data, 'gantry_origin')
        
        # 添加数据质量评估到结果中
        stats['data_quality_assessment'] = {
            'total_od_records': len(correlation_data),
            'matched_records': matched_records,
            'missing_flow_records': missing_flow_count,
            'match_rate': matched_records / len(correlation_data) if len(correlation_data) > 0 else 0,
            'common_stations_count': len(common_stations),
            'od_only_stations_count': len(od_only_stations),
            'flow_only_stations_count': len(flow_only_stations)
        }

        # 导出数据
        output_dir = os.path.join(os.path.dirname(__file__), 'detailed_correlation_output')
        os.makedirs(output_dir, exist_ok=True)
        
        gantry_file = os.path.join(output_dir, 'detailed_gantry_origin_correlation.json')
        self._save_json_results(stats, gantry_file)
        logger.info(f"门架起点关联分析数据已导出: {gantry_file}")

        return stats
    
    def _calculate_detailed_stats(self, correlation_data, analysis_type):
        """计算详细统计信息"""
        # 分离有效数据和所有数据进行不同的统计
        valid_data = correlation_data[correlation_data['flow_total'].notna()].copy()
        
        if len(valid_data) == 0:
            logger.warning(f"没有有效的匹配数据用于{analysis_type}统计")
            return {}
        
        # 修正计算逻辑：对于收费广场，流量应该与OD数量高度一致
        if 'toll_square' in analysis_type:
            # 对于收费广场，计算流量/OD比（应该接近1.0）
            valid_data['flow_od_ratio'] = valid_data['flow_total'] / valid_data['od_count']
            valid_data['od_flow_ratio'] = valid_data['od_count'] / valid_data['flow_total']
            
            # 数据质量评估
            valid_data['data_quality_flag'] = valid_data['od_flow_ratio'].apply(
                lambda x: 'normal' if 0.8 <= x <= 1.2 else 'abnormal'
            )
        else:
            # 对于门架，计算OD/流量比（预期较低，因为存在途中流量）
            valid_data['od_flow_ratio'] = valid_data['od_count'] / valid_data['flow_total']
            valid_data['flow_od_ratio'] = valid_data['flow_total'] / valid_data['od_count']
        
        # 用valid_data计算比值，correlation_data填充NaN为0用于其他统计
        correlation_data['od_flow_ratio'] = correlation_data['od_count'] / correlation_data['flow_total']
        correlation_data['flow_od_ratio'] = correlation_data['flow_total'] / correlation_data['od_count']
        correlation_data['od_flow_ratio'] = correlation_data['od_flow_ratio'].fillna(0)
        correlation_data['flow_od_ratio'] = correlation_data['flow_od_ratio'].fillna(0)
        
        # 车型占比计算
        correlation_data['flow_passenger_ratio'] = correlation_data['flow_k'] / correlation_data['flow_total']
        correlation_data['flow_truck_ratio'] = correlation_data['flow_h'] / correlation_data['flow_total']
        correlation_data['flow_trailer_ratio'] = correlation_data['flow_t'] / correlation_data['flow_total']
        
        # 提取OD车型占比
        def extract_vehicle_ratios(vehicle_dist):
            if not isinstance(vehicle_dist, dict):
                return {'passenger_ratio': 0, 'truck_ratio': 0, 'trailer_ratio': 0}
            total = sum(vehicle_dist.values())
            if total == 0:
                return {'passenger_ratio': 0, 'truck_ratio': 0, 'trailer_ratio': 0}
            return {
                'passenger_ratio': vehicle_dist.get('passenger', 0) / total,
                'truck_ratio': vehicle_dist.get('truck', 0) / total,
                'trailer_ratio': vehicle_dist.get('trailer', 0) / total
            }

        # 检查od_vehicle_dist列是否存在
        if 'od_vehicle_dist' in correlation_data.columns:
            vehicle_ratios = correlation_data['od_vehicle_dist'].apply(extract_vehicle_ratios)
            vehicle_ratios_df = pd.json_normalize(vehicle_ratios)
            correlation_data = pd.concat([correlation_data, vehicle_ratios_df], axis=1)

            # 计算差异（只有当相关列存在时）
            if 'truck_ratio' in correlation_data.columns and 'flow_truck_ratio' in correlation_data.columns:
                correlation_data['truck_ratio_diff'] = correlation_data['truck_ratio'] - correlation_data['flow_truck_ratio']
            if 'passenger_ratio' in correlation_data.columns and 'flow_passenger_ratio' in correlation_data.columns:
                correlation_data['passenger_ratio_diff'] = correlation_data['passenger_ratio'] - correlation_data['flow_passenger_ratio']
        else:
            logger.warning("od_vehicle_dist列不存在，跳过车型占比计算")
        
        # 统计结果 - 使用valid_data计算关键指标
        stats = {
            'total_records': len(valid_data),  # 有效匹配记录数
            'total_od_records': len(correlation_data),  # 总OD记录数
            'unique_locations': valid_data['station_code'].nunique() if 'gantry' in analysis_type else valid_data['square_code'].nunique(),
            'date_range': {
                'start': valid_data['od_date'].min() if len(valid_data) > 0 else None,
                'end': valid_data['od_date'].max() if len(valid_data) > 0 else None
            },
            'correlation_coefficient': valid_data['od_count'].corr(valid_data['flow_total']),
            
            # 基于有效匹配数据的比值统计
            'od_flow_ratio_stats': {
                'mean': valid_data['od_flow_ratio'].mean(),
                'median': valid_data['od_flow_ratio'].median(),
                'std': valid_data['od_flow_ratio'].std(),
                'min': valid_data['od_flow_ratio'].min(),
                'max': valid_data['od_flow_ratio'].max(),
                'q25': valid_data['od_flow_ratio'].quantile(0.25),
                'q75': valid_data['od_flow_ratio'].quantile(0.75)
            },
            
            # 新增：流量/OD比统计（对收费广场更有意义）
            'flow_od_ratio_stats': {
                'mean': valid_data['flow_od_ratio'].mean(),
                'median': valid_data['flow_od_ratio'].median(),
                'std': valid_data['flow_od_ratio'].std(),
                'min': valid_data['flow_od_ratio'].min(),
                'max': valid_data['flow_od_ratio'].max(),
                'q25': valid_data['flow_od_ratio'].quantile(0.25),
                'q75': valid_data['flow_od_ratio'].quantile(0.75)
            }
        }
        
        # 收费广场特有的数据质量评估
        if 'toll_square' in analysis_type:
            quality_stats = valid_data['data_quality_flag'].value_counts().to_dict()
            stats['data_quality_assessment'] = {
                'normal_records': quality_stats.get('normal', 0),
                'abnormal_records': quality_stats.get('abnormal', 0),
                'normal_ratio': quality_stats.get('normal', 0) / len(valid_data) if len(valid_data) > 0 else 0,
                'expected_consistency': '流量应与OD数量高度一致(比值接近1.0)',
                'actual_consistency': f"平均流量/OD比: {stats['flow_od_ratio_stats']['mean']:.2f}"
            }
        
        # 车型比例对比（如果数据存在）
        if 'truck_ratio' in valid_data.columns:
            stats['vehicle_ratio_comparison'] = {
                'od_truck_ratio_mean': valid_data['truck_ratio'].mean(),
                'flow_truck_ratio_mean': valid_data['flow_truck_ratio'].mean(),
                'truck_ratio_diff_mean': valid_data['truck_ratio_diff'].mean(),
                'truck_ratio_diff_std': valid_data['truck_ratio_diff'].std(),
                'od_passenger_ratio_mean': valid_data['passenger_ratio'].mean(),
                'flow_passenger_ratio_mean': valid_data['flow_passenger_ratio'].mean(),
                'passenger_ratio_diff_mean': valid_data['passenger_ratio_diff'].mean(),
                'passenger_ratio_diff_std': valid_data['passenger_ratio_diff'].std()
            }
        
        return stats



    def _analyze_time_patterns(self, correlation_data):
        """分析时间模式"""
        patterns = {}

        # 基础聚合字段
        base_agg = {
            'od_count': 'mean',
            'flow_total': 'mean',
            'od_flow_ratio': 'mean'
        }

        # 添加可选的车型字段
        optional_fields = ['truck_ratio', 'flow_truck_ratio', 'passenger_ratio', 'flow_passenger_ratio']
        for field in optional_fields:
            if field in correlation_data.columns:
                base_agg[field] = 'mean'

        # 小时模式
        hourly_pattern = correlation_data.groupby('od_hour').agg(base_agg).round(4)
        patterns['hourly_pattern'] = hourly_pattern.to_dict()

        # 工作日vs周末模式（如果有足够数据）
        correlation_data['weekday'] = pd.to_datetime(correlation_data['od_date']).dt.dayofweek
        correlation_data['is_weekend'] = correlation_data['weekday'].isin([5, 6])

        weekend_pattern = correlation_data.groupby('is_weekend').agg(base_agg).round(4)
        patterns['weekend_pattern'] = weekend_pattern.to_dict()

        # 高峰时段识别
        peak_hours = hourly_pattern.nlargest(3, 'od_count').index.tolist()
        off_peak_hours = hourly_pattern.nsmallest(3, 'od_count').index.tolist()

        patterns['peak_analysis'] = {
            'peak_hours': peak_hours,
            'off_peak_hours': off_peak_hours,
            'peak_avg_od_flow_ratio': correlation_data[correlation_data['od_hour'].isin(peak_hours)]['od_flow_ratio'].mean(),
            'off_peak_avg_od_flow_ratio': correlation_data[correlation_data['od_hour'].isin(off_peak_hours)]['od_flow_ratio'].mean()
        }

        return patterns

    def analyze_gantry_destination_correlation(self):
        """门架作为终点的关联分析"""
        logger.info("开始门架终点关联分析")

        # 筛选门架终点的OD数据
        gantry_od = self.od_data[self.od_data['end_point_type'] == 'gantry'].copy()

        if len(gantry_od) == 0:
            logger.warning("没有门架终点的OD数据")
            return {}

        # 按门架、日期、小时聚合OD数据
        gantry_od['end_date'] = gantry_od['end_datetime'].dt.date
        gantry_od['end_hour'] = gantry_od['end_datetime'].dt.hour

        od_aggregated = gantry_od.groupby(['end_point_code', 'end_date', 'end_hour']).agg({
            'pass_id': 'count',
            'vehicle_class': lambda x: x.value_counts().to_dict()
        }).reset_index()
        od_aggregated.columns = ['station_code', 'od_date', 'od_hour', 'od_count', 'od_vehicle_dist']

        # 按门架、日期、小时聚合流量数据
        gantry_flow = self.gantry_flow_data.copy()
        gantry_flow['flow_date'] = pd.to_datetime(gantry_flow['start_time']).dt.date
        gantry_flow['flow_hour'] = pd.to_datetime(gantry_flow['start_time']).dt.hour

        flow_aggregated = gantry_flow.groupby(['station_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum',
            'total_k': 'sum',
            'total_h': 'sum',
            'total_t': 'sum'
        }).reset_index()
        flow_aggregated.columns = ['station_code', 'flow_date', 'flow_hour', 'flow_total', 'flow_k', 'flow_h', 'flow_t']

        # 使用left join保留所有OD数据
        correlation_data = pd.merge(
            od_aggregated, flow_aggregated,
            left_on=['station_code', 'od_date', 'od_hour'],
            right_on=['station_code', 'flow_date', 'flow_hour'],
            how='left'
        )

        if len(correlation_data) == 0:
            logger.warning("门架终点OD数据与流量数据无法关联")
            return {}

        # 计算详细统计
        results = self._calculate_detailed_stats(correlation_data, 'gantry_destination')

        logger.info("门架终点关联分析完成")
        return results

    def _analyze_vehicle_structure(self, correlation_data):
        """分析车型结构"""
        structure_analysis = {}

        # 检查必要的字段是否存在
        required_fields = ['passenger_ratio', 'flow_passenger_ratio', 'truck_ratio', 'flow_truck_ratio']
        diff_fields = ['passenger_ratio_diff', 'truck_ratio_diff']

        # 整体车型占比对比
        overall_comparison = {}

        # 只添加存在的字段
        if 'passenger_ratio' in correlation_data.columns:
            overall_comparison['od_passenger_ratio'] = correlation_data['passenger_ratio'].mean()
        if 'flow_passenger_ratio' in correlation_data.columns:
            overall_comparison['flow_passenger_ratio'] = correlation_data['flow_passenger_ratio'].mean()
        if 'truck_ratio' in correlation_data.columns:
            overall_comparison['od_truck_ratio'] = correlation_data['truck_ratio'].mean()
        if 'flow_truck_ratio' in correlation_data.columns:
            overall_comparison['flow_truck_ratio'] = correlation_data['flow_truck_ratio'].mean()
        if 'passenger_ratio_diff' in correlation_data.columns:
            overall_comparison['passenger_ratio_diff'] = correlation_data['passenger_ratio_diff'].mean()
        if 'truck_ratio_diff' in correlation_data.columns:
            overall_comparison['truck_ratio_diff'] = correlation_data['truck_ratio_diff'].mean()

        structure_analysis['overall_comparison'] = overall_comparison

        # 车型差异分布（只有当truck_ratio_diff存在时）
        if 'truck_ratio_diff' in correlation_data.columns:
            structure_analysis['ratio_diff_distribution'] = {
                'truck_ratio_diff_stats': {
                    'mean': correlation_data['truck_ratio_diff'].mean(),
                    'std': correlation_data['truck_ratio_diff'].std(),
                    'min': correlation_data['truck_ratio_diff'].min(),
                    'max': correlation_data['truck_ratio_diff'].max(),
                    'q25': correlation_data['truck_ratio_diff'].quantile(0.25),
                    'q75': correlation_data['truck_ratio_diff'].quantile(0.75)
                }
            }

        # 按时段的车型结构差异
        hourly_agg_dict = {}
        for field in diff_fields:
            if field in correlation_data.columns:
                hourly_agg_dict[field] = ['mean', 'std']

        if hourly_agg_dict:
            hourly_vehicle_diff = correlation_data.groupby('hour').agg(hourly_agg_dict).round(4)
            structure_analysis['hourly_vehicle_diff'] = hourly_vehicle_diff.to_dict()

        return structure_analysis

    def analyze_gantry_transit_flow(self):
        """分析门架途中流量"""
        logger.info("开始门架途中流量分析")

        # 获取门架起点和终点的OD统计
        gantry_origin_stats = self._get_gantry_od_stats('origin')
        gantry_destination_stats = self._get_gantry_od_stats('destination')

        # 获取门架流量统计
        gantry_flow_stats = self._get_gantry_flow_stats()

        # 计算途中流量
        transit_analysis = self._calculate_transit_flow(
            gantry_flow_stats, gantry_origin_stats, gantry_destination_stats
        )

        logger.info("门架途中流量分析完成")
        return transit_analysis

    def _get_gantry_od_stats(self, point_type):
        """获取门架OD统计（起点或终点）"""
        if point_type == 'origin':
            gantry_od = self.od_data[self.od_data['start_point_type'] == 'gantry'].copy()
            gantry_od['point_code'] = gantry_od['start_point_code']
            gantry_od['od_date'] = gantry_od['start_datetime'].dt.date
            gantry_od['od_hour'] = gantry_od['start_datetime'].dt.hour
        else:  # destination
            # 需要先添加end_point_type字段
            self.od_data['end_point_type'] = self.od_data['end_square_code'].apply(
                lambda x: 'toll_square' if pd.notna(x) else 'gantry'
            )
            gantry_od = self.od_data[self.od_data['end_point_type'] == 'gantry'].copy()
            gantry_od['point_code'] = gantry_od['end_point_code']
            gantry_od['end_datetime'] = pd.to_datetime(gantry_od['end_time'])
            gantry_od['od_date'] = gantry_od['end_datetime'].dt.date
            gantry_od['od_hour'] = gantry_od['end_datetime'].dt.hour

        od_stats = gantry_od.groupby(['point_code', 'od_date', 'od_hour']).agg({
            'pass_id': 'count'
        }).reset_index()
        od_stats.columns = ['gantry_code', 'date', 'hour', f'{point_type}_od_count']

        return od_stats

    def _get_gantry_flow_stats(self):
        """获取门架流量统计"""
        gantry_flow = self.gantry_flow_data.copy()
        gantry_flow['flow_date'] = pd.to_datetime(gantry_flow['start_time']).dt.date
        gantry_flow['flow_hour'] = pd.to_datetime(gantry_flow['start_time']).dt.hour

        flow_stats = gantry_flow.groupby(['station_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum'
        }).reset_index()
        flow_stats.columns = ['gantry_code', 'date', 'hour', 'total_flow']

        return flow_stats

    def _calculate_transit_flow(self, flow_stats, origin_stats, destination_stats):
        """计算途中流量"""
        # 合并所有数据
        combined_data = flow_stats.copy()

        # 左连接起点OD统计
        combined_data = pd.merge(
            combined_data, origin_stats,
            on=['gantry_code', 'date', 'hour'],
            how='left'
        )

        # 左连接终点OD统计
        combined_data = pd.merge(
            combined_data, destination_stats,
            on=['gantry_code', 'date', 'hour'],
            how='left'
        )

        # 填充缺失值
        combined_data['origin_od_count'] = combined_data['origin_od_count'].fillna(0)
        combined_data['destination_od_count'] = combined_data['destination_od_count'].fillna(0)

        # 计算途中流量
        combined_data['od_related_flow'] = combined_data['origin_od_count'] + combined_data['destination_od_count']
        combined_data['transit_flow'] = combined_data['total_flow'] - combined_data['od_related_flow']
        combined_data['transit_flow'] = combined_data['transit_flow'].clip(lower=0)  # 确保非负

        # 计算比例
        combined_data['od_ratio'] = combined_data['od_related_flow'] / combined_data['total_flow']
        combined_data['transit_ratio'] = combined_data['transit_flow'] / combined_data['total_flow']

        # 统计分析
        transit_analysis = {
            'total_records': len(combined_data),
            'unique_gantries': combined_data['gantry_code'].nunique(),
            'avg_transit_ratio': combined_data['transit_ratio'].mean(),
            'avg_od_ratio': combined_data['od_ratio'].mean(),
            'transit_ratio_stats': {
                'mean': combined_data['transit_ratio'].mean(),
                'median': combined_data['transit_ratio'].median(),
                'std': combined_data['transit_ratio'].std(),
                'min': combined_data['transit_ratio'].min(),
                'max': combined_data['transit_ratio'].max(),
                'q25': combined_data['transit_ratio'].quantile(0.25),
                'q75': combined_data['transit_ratio'].quantile(0.75)
            },
            'gantry_function_classification': self._classify_gantry_function(combined_data)
        }

        return transit_analysis

    def _classify_gantry_function(self, combined_data):
        """门架功能分类"""
        # 按门架分组统计
        gantry_summary = combined_data.groupby('gantry_code').agg({
            'transit_ratio': 'mean',
            'od_ratio': 'mean',
            'total_flow': 'sum'
        }).reset_index()

        # 功能分类
        def classify_function(row):
            if row['od_ratio'] > 0.5:
                return '起止型'  # OD功能为主
            elif row['transit_ratio'] > 0.8:
                return '通道型'  # 途中流量为主
            else:
                return '混合型'  # 混合功能

        gantry_summary['function_type'] = gantry_summary.apply(classify_function, axis=1)

        # 统计各类型数量
        function_stats = gantry_summary['function_type'].value_counts().to_dict()

        return {
            'function_distribution': function_stats,
            'gantry_details': gantry_summary.to_dict('records')
        }

    def generate_enhanced_report(self, all_results, output_file):
        """生成增强版报告"""
        logger.info(f"生成增强版报告: {output_file}")

        # 生成完整的增强版报告
        html_content = self._generate_enhanced_html_report(all_results)

        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # 写入HTML文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"增强版报告已生成: {output_file}")

    def _generate_enhanced_html_report(self, all_results):
        """生成增强版HTML报告"""
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>高速公路OD与流量关联分析报告（增强版）</title>
            <style>
                body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .section {{ margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }}
                .section h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                .section h3 {{ color: #34495e; margin-top: 25px; }}
                .section h4 {{ color: #7f8c8d; margin-top: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f8f9fa; font-weight: bold; }}
                .highlight {{ background-color: #fff3cd; }}
                .success {{ color: #28a745; }}
                .warning {{ color: #ffc107; }}
                .danger {{ color: #dc3545; }}
                .info {{ color: #17a2b8; }}
                ul {{ margin: 10px 0; padding-left: 20px; }}
                .summary-box {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>高速公路OD与流量关联分析报告（增强版）</h1>
                <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        """

        # 门架分析部分
        html += self._generate_enhanced_gantry_section(all_results)

        # 收费广场分析部分
        html += self._generate_enhanced_toll_square_section(all_results)

        # 对比分析部分
        html += self._generate_enhanced_comparison_section(all_results)

        html += """
        </body>
        </html>
        """

        return html

    def _generate_enhanced_gantry_section(self, all_results):
        """生成增强版门架分析部分"""
        html = """
        <div class="section">
            <h2>1. 门架关联分析</h2>
        """

        # 门架起点分析
        gantry_origin = all_results.get('gantry_origin', {})
        if gantry_origin:
            html += f"""
            <h3>1.1 门架起点分析</h3>
            <div class="summary-box">
                <p><strong>基本统计：</strong></p>
                <ul>
                    <li>关联记录数: {gantry_origin.get('total_records', 0):,}</li>
                    <li>涉及门架数: {gantry_origin.get('unique_locations', 0)}</li>
                    <li>相关系数: {gantry_origin.get('correlation_coefficient', 0):.3f}</li>
                    <li>平均OD/流量比: {gantry_origin.get('od_flow_ratio_stats', {}).get('mean', 0):.3f}</li>
                </ul>
            </div>
            """

        # 门架终点分析
        gantry_destination = all_results.get('gantry_destination', {})
        if gantry_destination:
            html += f"""
            <h3>1.2 门架终点分析</h3>
            <div class="summary-box">
                <p><strong>基本统计：</strong></p>
                <ul>
                    <li>关联记录数: {gantry_destination.get('total_records', 0):,}</li>
                    <li>涉及门架数: {gantry_destination.get('unique_locations', 0)}</li>
                    <li>相关系数: {gantry_destination.get('correlation_coefficient', 0):.3f}</li>
                    <li>平均OD/流量比: {gantry_destination.get('od_flow_ratio_stats', {}).get('mean', 0):.3f}</li>
                </ul>
            </div>
            """

        # 门架途中流量分析
        gantry_transit = all_results.get('gantry_transit', {})
        if gantry_transit:
            function_dist = gantry_transit.get('gantry_function_classification', {}).get('function_distribution', {})
            html += f"""
            <h3>1.3 门架途中流量分析</h3>
            <div class="summary-box">
                <p><strong>途中流量统计：</strong></p>
                <ul>
                    <li>涉及门架数: {gantry_transit.get('unique_gantries', 0)}</li>
                    <li>平均途中流量占比: {gantry_transit.get('avg_transit_ratio', 0):.1%}</li>
                    <li>平均OD占比: {gantry_transit.get('avg_od_ratio', 0):.1%}</li>
                </ul>
                <p><strong>门架功能分类：</strong></p>
                <ul>
                    <li>通道型门架: {function_dist.get('通道型', 0)}个</li>
                    <li>混合型门架: {function_dist.get('混合型', 0)}个</li>
                    <li>起止型门架: {function_dist.get('起止型', 0)}个</li>
                </ul>
            </div>
            """

        html += "</div>"
        return html

    def _generate_enhanced_toll_square_section(self, all_results):
        """生成增强版收费广场分析部分"""
        html = """
        <div class="section">
            <h2>2. 收费广场关联分析</h2>
        """

        # 收费广场入口分析
        toll_square_entry = all_results.get('toll_square_entry', {})
        if toll_square_entry:
            od_flow_stats = toll_square_entry.get('od_flow_ratio_stats', {})
            html += f"""
            <h3>2.1 收费广场入口分析</h3>
            <div class="summary-box">
                <p><strong>基本统计：</strong></p>
                <ul>
                    <li>关联记录数: {toll_square_entry.get('total_records', 0):,}</li>
                    <li>涉及收费广场数: {toll_square_entry.get('unique_locations', 0)}</li>
                    <li>相关系数: {toll_square_entry.get('correlation_coefficient', 0):.3f}</li>
                    <li>平均OD/流量比: {od_flow_stats.get('mean', 0):.3f}</li>
                    <li>中位数OD/流量比: {od_flow_stats.get('median', 0):.3f}</li>
                </ul>
                <p><strong>OD/流量比分布：</strong></p>
                <ul>
                    <li>最小值: {od_flow_stats.get('min', 0):.3f}</li>
                    <li>25%分位数: {od_flow_stats.get('q25', 0):.3f}</li>
                    <li>75%分位数: {od_flow_stats.get('q75', 0):.3f}</li>
                    <li>最大值: {od_flow_stats.get('max', 0):.3f}</li>
                    <li>标准差: {od_flow_stats.get('std', 0):.3f}</li>
                </ul>
            </div>
            """

            # 添加车型结构分析
            vehicle_analysis = toll_square_entry.get('vehicle_structure_analysis', {})
            if vehicle_analysis:
                overall_comp = vehicle_analysis.get('overall_comparison', {})
                html += f"""
                <h4>车型结构对比</h4>
                <table>
                    <tr><th>车型</th><th>OD数据占比</th><th>流量数据占比</th><th>差异</th></tr>
                """

                if 'od_truck_ratio' in overall_comp and 'flow_truck_ratio' in overall_comp:
                    truck_diff = overall_comp.get('truck_ratio_diff', 0)
                    html += f"""
                    <tr>
                        <td>货车</td>
                        <td>{overall_comp.get('od_truck_ratio', 0):.1%}</td>
                        <td>{overall_comp.get('flow_truck_ratio', 0):.1%}</td>
                        <td>{truck_diff:+.1%}</td>
                    </tr>
                    """

                if 'od_passenger_ratio' in overall_comp and 'flow_passenger_ratio' in overall_comp:
                    passenger_diff = overall_comp.get('passenger_ratio_diff', 0)
                    html += f"""
                    <tr>
                        <td>客车</td>
                        <td>{overall_comp.get('od_passenger_ratio', 0):.1%}</td>
                        <td>{overall_comp.get('flow_passenger_ratio', 0):.1%}</td>
                        <td>{passenger_diff:+.1%}</td>
                    </tr>
                    """

                html += "</table>"

        # 收费广场出口分析
        toll_square_exit = all_results.get('toll_square_exit', {})
        if toll_square_exit:
            od_flow_stats = toll_square_exit.get('od_flow_ratio_stats', {})
            html += f"""
            <h3>2.2 收费广场出口分析</h3>
            <div class="summary-box">
                <p><strong>基本统计：</strong></p>
                <ul>
                    <li>关联记录数: {toll_square_exit.get('total_records', 0):,}</li>
                    <li>涉及收费广场数: {toll_square_exit.get('unique_locations', 0)}</li>
                    <li>相关系数: {toll_square_exit.get('correlation_coefficient', 0):.3f}</li>
                    <li>平均OD/流量比: {od_flow_stats.get('mean', 0):.3f}</li>
                    <li>中位数OD/流量比: {od_flow_stats.get('median', 0):.3f}</li>
                </ul>
                <p><strong>OD/流量比分布：</strong></p>
                <ul>
                    <li>最小值: {od_flow_stats.get('min', 0):.3f}</li>
                    <li>25%分位数: {od_flow_stats.get('q25', 0):.3f}</li>
                    <li>75%分位数: {od_flow_stats.get('q75', 0):.3f}</li>
                    <li>最大值: {od_flow_stats.get('max', 0):.3f}</li>
                    <li>标准差: {od_flow_stats.get('std', 0):.3f}</li>
                </ul>
            </div>
            """

            # 添加车型结构分析
            vehicle_analysis = toll_square_exit.get('vehicle_structure_analysis', {})
            if vehicle_analysis:
                overall_comp = vehicle_analysis.get('overall_comparison', {})
                html += f"""
                <h4>车型结构对比</h4>
                <table>
                    <tr><th>车型</th><th>OD数据占比</th><th>流量数据占比</th><th>差异</th></tr>
                """

                if 'od_truck_ratio' in overall_comp and 'flow_truck_ratio' in overall_comp:
                    truck_diff = overall_comp.get('truck_ratio_diff', 0)
                    html += f"""
                    <tr>
                        <td>货车</td>
                        <td>{overall_comp.get('od_truck_ratio', 0):.1%}</td>
                        <td>{overall_comp.get('flow_truck_ratio', 0):.1%}</td>
                        <td>{truck_diff:+.1%}</td>
                    </tr>
                    """

                if 'od_passenger_ratio' in overall_comp and 'flow_passenger_ratio' in overall_comp:
                    passenger_diff = overall_comp.get('passenger_ratio_diff', 0)
                    html += f"""
                    <tr>
                        <td>客车</td>
                        <td>{overall_comp.get('od_passenger_ratio', 0):.1%}</td>
                        <td>{overall_comp.get('flow_passenger_ratio', 0):.1%}</td>
                        <td>{passenger_diff:+.1%}</td>
                    </tr>
                    """

                html += "</table>"

        # 收费广场流量平衡分析
        toll_square_balance = all_results.get('toll_square_balance', {})
        if toll_square_balance:
            balance_stats = toll_square_balance.get('balance_ratio_stats', {})
            imbalanced = toll_square_balance.get('imbalanced_squares', {})
            html += f"""
            <h3>2.3 收费广场流量平衡分析</h3>
            <div class="summary-box">
                <p><strong>平衡统计：</strong></p>
                <ul>
                    <li>涉及收费广场数: {toll_square_balance.get('unique_squares', 0)}</li>
    
                    <li>中位数进出流量比: {balance_stats.get('median', 0):.3f}</li>
                    <li>不平衡收费广场数: {imbalanced.get('count', 0)}</li>
                    <li>不平衡比例: {imbalanced.get('count', 0) / max(toll_square_balance.get('unique_squares', 1), 1):.1%}</li>
                </ul>
            </div>
            """

        html += "</div>"
        return html

    def _generate_enhanced_comparison_section(self, all_results):
        """生成增强版对比分析部分"""
        html = """
        <div class="section">
            <h2>3. 关键发现与分析</h2>
        """

        gantry_origin = all_results.get('gantry_origin', {})
        gantry_transit = all_results.get('gantry_transit', {})
        toll_square_entry = all_results.get('toll_square_entry', {})
        toll_square_exit = all_results.get('toll_square_exit', {})

        html += """
        <h3>3.1 门架途中流量验证</h3>
        <div class="summary-box">
        """

        if gantry_transit:
            html += f"""
            <p><strong>门架途中流量分析验证了理论假设：</strong></p>
            <ul>
                <li>平均途中流量占比高达 <span class="info">{gantry_transit.get('avg_transit_ratio', 0):.1%}</span></li>
                <li>平均OD相关流量占比仅为 <span class="info">{gantry_transit.get('avg_od_ratio', 0):.1%}</span></li>
                <li>这说明门架主要承担<strong>通道功能</strong>，而非起止点功能</li>
            </ul>
            """

        html += """
        </div>

        <h3>3.2 收费广场一致性验证</h3>
        <div class="summary-box">
        """

        if toll_square_entry:
            entry_corr = toll_square_entry.get('correlation_coefficient', 0)
            html += f"""
            <p><strong>收费广场入口分析验证了高一致性假设：</strong></p>
            <ul>
                <li>入口流量与OD数据相关系数: <span class="success">{entry_corr:.3f}</span></li>
                <li>这说明收费广场入口流量与以该广场为起点的OD数据高度一致</li>
            </ul>
            """

        if toll_square_exit:
            exit_corr = toll_square_exit.get('correlation_coefficient', 0)
            html += f"""
            <p><strong>收费广场出口分析结果：</strong></p>
            <ul>
                <li>出口流量与OD数据相关系数: <span class="success">{exit_corr:.3f}</span></li>
                <li>这说明收费广场出口流量与以该广场为终点的OD数据也高度一致</li>
            </ul>
            """

        html += """
        </div>

        <h3>3.3 数据质量评估</h3>
        <div class="summary-box">
        """

        if toll_square_entry:
            median_ratio = toll_square_entry.get('od_flow_ratio_stats', {}).get('median', 0)
            html += f"""
            <p><strong>收费广场入口OD/流量比中位数为{median_ratio:.3f}的原因分析：</strong></p>
            <ul>
                <li>中位数约为50%，说明存在系统性差异</li>
                <li>可能原因：OD数据采样不完整，或流量数据包含了部分非OD记录的车辆</li>
                <li>建议：进一步分析中位数附近的具体案例，识别差异来源</li>
            </ul>
            """

        # 添加中位数案例分析
        toll_square_median_entry = all_results.get('toll_square_median_entry', {})
        toll_square_median_exit = all_results.get('toll_square_median_exit', {})

        if toll_square_median_entry:
            html += f"""
            <h4>收费广场入口中位数案例分析</h4>
            <p><strong>中位数OD/流量比: {toll_square_median_entry.get('median_ratio', 0):.3f}</strong></p>
            <p>分析了{toll_square_median_entry.get('median_range_cases', 0)}个中位数附近的案例，以下是代表性样本：</p>
            <table>
                <tr><th>收费广场</th><th>日期</th><th>小时</th><th>OD数量</th><th>流量总数</th><th>OD/流量比</th></tr>
            """

            sample_cases = toll_square_median_entry.get('sample_cases', [])
            for case in sample_cases[:5]:  # 显示前5个案例
                html += f"""
                <tr>
                    <td>{case.get('square_code', '')}</td>
                    <td>{case.get('date', '')}</td>
                    <td>{case.get('hour', '')}</td>
                    <td>{case.get('od_count', 0)}</td>
                    <td>{case.get('flow_total', 0)}</td>
                    <td>{case.get('od_flow_ratio', 0):.3f}</td>
                </tr>
                """

            html += "</table>"

        if toll_square_median_exit:
            html += f"""
            <h4>收费广场出口中位数案例分析</h4>
            <p><strong>中位数OD/流量比: {toll_square_median_exit.get('median_ratio', 0):.3f}</strong></p>
            <p>分析了{toll_square_median_exit.get('median_range_cases', 0)}个中位数附近的案例，以下是代表性样本：</p>
            <table>
                <tr><th>收费广场</th><th>日期</th><th>小时</th><th>OD数量</th><th>流量总数</th><th>OD/流量比</th></tr>
            """

            sample_cases = toll_square_median_exit.get('sample_cases', [])
            for case in sample_cases[:5]:  # 显示前5个案例
                html += f"""
                <tr>
                    <td>{case.get('square_code', '')}</td>
                    <td>{case.get('date', '')}</td>
                    <td>{case.get('hour', '')}</td>
                    <td>{case.get('od_count', 0)}</td>
                    <td>{case.get('flow_total', 0)}</td>
                    <td>{case.get('od_flow_ratio', 0):.3f}</td>
                </tr>
                """

            html += "</table>"

        html += """
        </div>
        </div>
        """

        return html

    def export_enhanced_data(self, all_results, output_dir):
        """导出增强版数据"""
        logger.info(f"导出增强版数据到: {output_dir}")

        import json
        os.makedirs(output_dir, exist_ok=True)

        # 导出各个分析结果
        for analysis_type, results in all_results.items():
            if results:
                output_file = os.path.join(output_dir, f"{analysis_type}_analysis.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                logger.info(f"导出{analysis_type}分析结果: {output_file}")

    def analyze_toll_square_correlation_detailed(self):
        """详细的收费广场入口关联分析"""
        logger.info("开始详细收费广场入口关联分析")

        # 筛选收费广场起点的OD数据（终点不受限制）
        toll_square_od = self.od_data[self.od_data['start_point_type'] == 'toll_square'].copy()

        if len(toll_square_od) == 0:
            logger.warning("没有收费广场起点的OD数据")
            return {}

        # 基础数据量统计
        logger.info(f"收费广场起点OD数据总数: {len(toll_square_od)}")
        logger.info(f"收费广场入口流量数据总数: {len(self.onramp_flow_data)}")
        
        # 处理收费广场流量数据（onramp表本身就是入口流量，无需筛选direction）
        onramp_flow = self.onramp_flow_data.copy()
        onramp_flow['flow_datetime'] = pd.to_datetime(onramp_flow['start_time'])
        onramp_flow['flow_hour'] = onramp_flow['flow_datetime'].dt.hour
        onramp_flow['flow_date'] = onramp_flow['flow_datetime'].dt.date

        # 按收费广场、日期、小时聚合OD数据
        od_aggregated = toll_square_od.groupby(['start_square_code', 'start_date', 'start_hour']).agg({
            'pass_id': 'count',
            'vehicle_class': lambda x: x.value_counts().to_dict()
        }).reset_index()
        od_aggregated.columns = ['square_code', 'od_date', 'od_hour', 'od_count', 'od_vehicle_dist']

        # 按收费广场、日期、小时聚合流量数据
        flow_aggregated = onramp_flow.groupby(['square_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum',
            'total_k': 'sum',
            'total_h': 'sum',
            'total_t': 'sum'
        }).reset_index()
        flow_aggregated.columns = ['square_code', 'flow_date', 'flow_hour', 'flow_total', 'flow_k', 'flow_h', 'flow_t']

        # 数据质量诊断
        od_squares = set(od_aggregated['square_code'].unique())
        flow_squares = set(flow_aggregated['square_code'].unique())
        common_squares = od_squares.intersection(flow_squares)
        od_only_squares = od_squares - flow_squares
        flow_only_squares = flow_squares - od_squares
        
        logger.info(f"OD数据涉及收费广场数: {len(od_squares)}")
        logger.info(f"流量数据涉及收费广场数: {len(flow_squares)}")
        logger.info(f"共同收费广场数: {len(common_squares)}")
        
        if od_only_squares:
            logger.warning(f"仅OD有的收费广场数: {len(od_only_squares)}, 示例: {list(od_only_squares)[:3]}")
        if flow_only_squares:
            logger.warning(f"仅流量有的收费广场数: {len(flow_only_squares)}, 示例: {list(flow_only_squares)[:3]}")

        # 使用left join保留所有OD数据，但在统计时区分有效数据
        correlation_data = pd.merge(
            od_aggregated, flow_aggregated,
            left_on=['square_code', 'od_date', 'od_hour'],
            right_on=['square_code', 'flow_date', 'flow_hour'],
            how='left'  # 改回left join，保留所有OD记录
        )
        
        if len(correlation_data) == 0:
            logger.warning("关联后没有数据")
            return {}

        # 数据质量评估 - 由于使用inner join，所有记录都有匹配的流量数据
        missing_flow_count = 0  # inner join没有缺失流量记录
        matched_records = len(correlation_data)
        unmatched_od_count = len(od_aggregated) - matched_records  # 未匹配的OD记录数
        
        logger.info(f"数据关联结果统计:")
        logger.info(f"  - 有效关联记录数: {matched_records}")
        logger.info(f"  - 未匹配的OD记录数: {unmatched_od_count}")
        logger.info(f"  - OD记录匹配率: {matched_records/len(od_aggregated)*100:.1f}%")
        logger.info(f"  - OD总数: {correlation_data['od_count'].sum()}")
        
        if matched_records > 0:
            matched_od_count = correlation_data['od_count'].sum()
            matched_flow_total = correlation_data['flow_total'].sum()
            logger.info(f"  - 匹配部分OD总数: {matched_od_count}")
            logger.info(f"  - 匹配部分流量总数: {matched_flow_total}")
            if matched_flow_total > 0:
                logger.info(f"  - 匹配部分OD/流量比: {matched_od_count / matched_flow_total:.3f}")
        
        # 分析未匹配的原因
        if unmatched_od_count > 0:
            unmatched_squares = set(od_aggregated['square_code'].unique()) - set(correlation_data['square_code'].unique())
            logger.warning(f"未匹配原因分析:")
            logger.warning(f"  - 涉及{len(unmatched_squares)}个收费广场缺失流量数据")
            logger.warning(f"  - 未匹配收费广场示例: {list(unmatched_squares)[:5]}")

        # 计算详细统计
        stats = self._calculate_detailed_stats(correlation_data, 'toll_square_entry')
        
        # 添加数据质量评估到结果中
        stats['data_quality_assessment'] = {
            'total_od_records': len(od_aggregated),
            'matched_records': matched_records,
            'unmatched_od_records': unmatched_od_count,
            'od_match_rate': matched_records / len(od_aggregated) if len(od_aggregated) > 0 else 0,
            'common_squares_count': len(common_squares),
            'od_only_squares_count': len(od_only_squares),
            'flow_only_squares_count': len(flow_only_squares),
            'data_filter_note': '使用left join保留所有OD记录，统计基于有效匹配数据'
        }

        # 导出数据
        output_dir = os.path.join(os.path.dirname(__file__), 'detailed_correlation_output')
        os.makedirs(output_dir, exist_ok=True)
        
        toll_square_file = os.path.join(output_dir, 'detailed_toll_square_correlation.json')
        self._save_json_results(stats, toll_square_file)
        logger.info(f"收费广场关联分析数据已导出: {toll_square_file}")

        return stats

    def analyze_toll_square_exit_correlation(self):
        """收费广场出口关联分析"""
        logger.info("开始收费广场出口关联分析")

        # 筛选收费广场终点的OD数据
        toll_square_od = self.od_data[self.od_data['end_point_type'] == 'toll_square'].copy()

        if len(toll_square_od) == 0:
            logger.warning("没有收费广场终点的OD数据")
            return {}

        logger.info(f"过滤后用于分析的OD数据: {len(toll_square_od)} 条")
        logger.info(f"收费广场出口流量数据总数: {len(self.offramp_flow_data)}")

        # 检查end_time和start_time的时差分布
        toll_square_od['time_diff'] = (toll_square_od['end_datetime'] - toll_square_od['start_datetime']).dt.total_seconds() / 3600
        time_diff_stats = toll_square_od['time_diff'].describe()
        logger.info(f"行程时间统计(小时): 平均{time_diff_stats['mean']:.2f}, 中位数{time_diff_stats['50%']:.2f}, 最大{time_diff_stats['max']:.2f}")

        # 按收费广场、日期、小时聚合OD数据
        logger.info(f"开始聚合OD数据，聚合前记录数: {len(toll_square_od)}")
        
        # 检查聚合前数据的分布
        square_counts = toll_square_od['end_square_code'].value_counts()
        logger.info(f"聚合前收费广场分布: 总共{len(square_counts)}个收费广场")
        if 'G42015100200802020' in square_counts.index:
            logger.info(f"测试收费广场G42015100200802020在聚合前有 {square_counts['G42015100200802020']} 条记录")
        
        # 检查时间字段的情况
        time_check = toll_square_od[['end_square_code', 'end_date', 'end_hour']].copy()
        time_check_counts = time_check.groupby(['end_square_code', 'end_date', 'end_hour']).size()
        logger.info(f"时空组合统计: {len(time_check_counts)} 个唯一组合")
        
        # 执行聚合
        od_aggregated = toll_square_od.groupby(['end_square_code', 'end_date', 'end_hour']).agg({
            'pass_id': 'count',
            'vehicle_class': lambda x: x.value_counts().to_dict()
        }).reset_index()
        od_aggregated.columns = ['square_code', 'od_date', 'od_hour', 'od_count', 'od_vehicle_dist']

        logger.info(f"OD数据聚合结果: {len(od_aggregated)} 个时空组合")
        logger.info(f"聚合后总OD数量: {od_aggregated['od_count'].sum()}")
        
        # 检查特定收费广场的聚合结果
        test_square_data = od_aggregated[od_aggregated['square_code'] == 'G42015100200802020']
        if len(test_square_data) > 0:
            logger.info(f"测试收费广场G42015100200802020聚合结果:")
            for _, row in test_square_data.iterrows():
                logger.info(f"  {row['square_code']} {row['od_date']} {row['od_hour']}时: {row['od_count']} 条OD")
        else:
            logger.warning(f"测试收费广场G42015100200802020在聚合结果中不存在!")
        
        # 验证聚合正确性 - 随机检查几个收费广场
        sample_squares = od_aggregated['square_code'].unique()[:3]
        for square in sample_squares:
            square_data = od_aggregated[od_aggregated['square_code'] == square]
            total_od_for_square = square_data['od_count'].sum()
            
            # 在原始数据中验证
            original_count = len(toll_square_od[toll_square_od['end_square_code'] == square])
            logger.info(f"验证收费广场 {square}: 聚合后总数={total_od_for_square}, 原始记录数={original_count}")
            
            if total_od_for_square != original_count:
                logger.warning(f"数据聚合异常! 收费广场 {square} 聚合前后数量不一致")

        # 处理收费广场出口流量数据
        offramp_flow = self.offramp_flow_data.copy()
        offramp_flow['flow_datetime'] = pd.to_datetime(offramp_flow['start_time'])
        offramp_flow['flow_hour'] = offramp_flow['flow_datetime'].dt.hour
        offramp_flow['flow_date'] = offramp_flow['flow_datetime'].dt.date

        # 按收费广场、日期、小时聚合流量数据
        flow_aggregated = offramp_flow.groupby(['square_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum',
            'total_k': 'sum',
            'total_h': 'sum',
            'total_t': 'sum'
        }).reset_index()
        flow_aggregated.columns = ['square_code', 'flow_date', 'flow_hour', 'flow_total', 'flow_k', 'flow_h', 'flow_t']

        logger.info(f"流量数据聚合结果: {len(flow_aggregated)} 个时空组合")

        # 数据质量诊断
        od_squares = set(od_aggregated['square_code'].unique())
        flow_squares = set(flow_aggregated['square_code'].unique())
        common_squares = od_squares.intersection(flow_squares)
        od_only_squares = od_squares - flow_squares
        flow_only_squares = flow_squares - od_squares
        
        logger.info(f"收费广场代码匹配情况:")
        logger.info(f"  - OD数据涉及收费广场数: {len(od_squares)}")
        logger.info(f"  - 流量数据涉及收费广场数: {len(flow_squares)}")
        logger.info(f"  - 共同收费广场数: {len(common_squares)}")
        logger.info(f"  - 收费广场重叠率: {len(common_squares)/max(len(od_squares), 1)*100:.1f}%")
        
        if od_only_squares:
            logger.warning(f"仅OD有的收费广场数: {len(od_only_squares)}, 示例: {list(od_only_squares)[:3]}")
        if flow_only_squares:
            logger.warning(f"仅流量有的收费广场数: {len(flow_only_squares)}, 示例: {list(flow_only_squares)[:3]}")

        # 使用left join保留所有OD数据
        correlation_data = pd.merge(
            od_aggregated, flow_aggregated,
            left_on=['square_code', 'od_date', 'od_hour'],
            right_on=['square_code', 'flow_date', 'flow_hour'],
            how='left'
        )

        if len(correlation_data) == 0:
            logger.warning("收费广场出口关联数据为空")
            return {}

        # 数据质量评估
        missing_flow_count = correlation_data['flow_total'].isnull().sum()
        matched_records = len(correlation_data) - missing_flow_count
        
        logger.info(f"数据关联结果统计:")
        logger.info(f"  - 总记录数: {len(correlation_data)}")
        logger.info(f"  - 有流量匹配的记录数: {matched_records}")
        logger.info(f"  - 缺失流量的记录数: {missing_flow_count}")
        logger.info(f"  - 数据匹配率: {matched_records/len(correlation_data)*100:.1f}%")
        logger.info(f"  - OD总数: {correlation_data['od_count'].sum()}")
        
        if matched_records > 0:
            matched_od_count = correlation_data[correlation_data['flow_total'].notna()]['od_count'].sum()
            matched_flow_total = correlation_data['flow_total'].dropna().sum()
            logger.info(f"  - 匹配部分OD总数: {matched_od_count}")
            logger.info(f"  - 匹配部分流量总数: {matched_flow_total}")
            if matched_flow_total > 0:
                logger.info(f"  - 匹配部分OD/流量比: {matched_od_count / matched_flow_total:.3f}")
        
        # 分析未匹配的原因
        if missing_flow_count > 0:
            unmatched_data = correlation_data[correlation_data['flow_total'].isnull()]
            unmatched_squares = unmatched_data['square_code'].unique()
            logger.warning(f"未匹配流量的主要原因分析:")
            logger.warning(f"  - 涉及{len(unmatched_squares)}个收费广场缺失流量数据")
            logger.warning(f"  - 未匹配收费广场示例: {list(unmatched_squares)[:5]}")

        # 计算详细统计
        stats = self._calculate_detailed_stats(correlation_data, 'toll_square_exit')
        
        # 添加数据质量评估到结果中
        stats['data_quality_assessment'] = {
            'total_od_records': len(correlation_data),
            'matched_records': matched_records,
            'missing_flow_records': missing_flow_count,
            'match_rate': matched_records / len(correlation_data) if len(correlation_data) > 0 else 0,
            'common_squares_count': len(common_squares),
            'od_only_squares_count': len(od_only_squares),
            'flow_only_squares_count': len(flow_only_squares),
            'data_filter_note': '使用left join保留所有OD记录'
        }

        # 导出数据
        output_dir = os.path.join(os.path.dirname(__file__), 'detailed_correlation_output')
        os.makedirs(output_dir, exist_ok=True)
        
        toll_square_exit_file = os.path.join(output_dir, 'detailed_toll_square_exit_correlation.json')
        self._save_json_results(stats, toll_square_exit_file)
        logger.info(f"收费广场出口关联分析数据已导出: {toll_square_exit_file}")

        logger.info("收费广场出口关联分析完成")
        return stats

    def analyze_median_ratio_cases(self, analysis_type='entry'):
        """分析中位数OD/流量比的具体案例"""
        logger.info(f"开始分析收费广场{analysis_type}中位数比例案例")

        if analysis_type == 'entry':
            # 分析入口数据
            toll_square_od = self.od_data[self.od_data['start_point_type'] == 'toll_square'].copy()
            flow_data = self.onramp_flow_data.copy()
            time_field = 'start'
        else:
            # 分析出口数据
            toll_square_od = self.od_data[self.od_data['end_point_type'] == 'toll_square'].copy()
            flow_data = self.offramp_flow_data.copy()
            time_field = 'end'

        if len(toll_square_od) == 0:
            logger.warning(f"没有收费广场{analysis_type}的OD数据")
            return {}

        # 聚合OD数据
        if analysis_type == 'entry':
            od_aggregated = toll_square_od.groupby(['start_square_code', 'start_date', 'start_hour']).agg({
                'pass_id': 'count'
            }).reset_index()
            od_aggregated.columns = ['square_code', 'date', 'hour', 'od_count']
        else:
            od_aggregated = toll_square_od.groupby(['end_square_code', 'end_date', 'end_hour']).agg({
                'pass_id': 'count'
            }).reset_index()
            od_aggregated.columns = ['square_code', 'date', 'hour', 'od_count']

        # 聚合流量数据
        flow_data['flow_datetime'] = pd.to_datetime(flow_data['start_time'])
        flow_data['flow_hour'] = flow_data['flow_datetime'].dt.hour
        flow_data['flow_date'] = flow_data['flow_datetime'].dt.date

        flow_aggregated = flow_data.groupby(['square_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum'
        }).reset_index()
        flow_aggregated.columns = ['square_code', 'date', 'hour', 'flow_total']

        # 关联数据 - 使用left join保留所有OD数据
        correlation_data = pd.merge(
            od_aggregated, flow_aggregated,
            on=['square_code', 'date', 'hour'],
            how='left'
        )
        
        # 过滤出有流量匹配的记录用于中位数分析
        correlation_data_valid = correlation_data[correlation_data['flow_total'].notna()].copy()

        if len(correlation_data_valid) == 0:
            logger.warning(f"收费广场{analysis_type}没有有效的关联数据")
            return {}

        # 计算OD/流量比
        correlation_data_valid['od_flow_ratio'] = correlation_data_valid['od_count'] / correlation_data_valid['flow_total']

        # 找到中位数附近的案例
        median_ratio = correlation_data_valid['od_flow_ratio'].median()

        # 选择中位数附近的案例（±10%范围内）
        lower_bound = median_ratio * 0.9
        upper_bound = median_ratio * 1.1

        median_cases = correlation_data_valid[
            (correlation_data_valid['od_flow_ratio'] >= lower_bound) &
            (correlation_data_valid['od_flow_ratio'] <= upper_bound)
        ].copy()

        # 选择10个代表性案例
        sample_cases = median_cases.sample(min(10, len(median_cases)), random_state=42)

        # 获取详细的OD和流量数据
        detailed_cases = []
        for _, case in sample_cases.iterrows():
            square_code = case['square_code']
            date = case['date']
            hour = case['hour']

            # 获取该时段的详细OD数据
            if analysis_type == 'entry':
                od_detail = toll_square_od[
                    (toll_square_od['start_square_code'] == square_code) &
                    (toll_square_od['start_date'] == date) &
                    (toll_square_od['start_hour'] == hour)
                ].copy()
            else:
                od_detail = toll_square_od[
                    (toll_square_od['end_square_code'] == square_code) &
                    (toll_square_od['end_date'] == date) &
                    (toll_square_od['end_hour'] == hour)
                ].copy()

            # 获取该时段的详细流量数据
            flow_detail = flow_data[
                (flow_data['square_code'] == square_code) &
                (flow_data['flow_date'] == date) &
                (flow_data['flow_hour'] == hour)
            ].copy()

            case_info = {
                'square_code': square_code,
                'date': str(date),
                'hour': hour,
                'od_count': case['od_count'],
                'flow_total': case['flow_total'],
                'od_flow_ratio': case['od_flow_ratio'],
                'od_vehicle_types': od_detail['vehicle_type'].value_counts().to_dict() if len(od_detail) > 0 else {},
                'flow_breakdown': {
                    'total_k': flow_detail['total_k'].sum() if len(flow_detail) > 0 else 0,
                    'total_h': flow_detail['total_h'].sum() if len(flow_detail) > 0 else 0,
                    'total_t': flow_detail['total_t'].sum() if len(flow_detail) > 0 else 0
                }
            }
            detailed_cases.append(case_info)

        # 分析结果
        analysis_results = {
            'median_ratio': median_ratio,
            'case_count': len(median_cases),
            'sample_cases': detailed_cases,
            'analysis_summary': {
                'median_od_flow_ratio': median_ratio,
                'case_range': f'{lower_bound:.3f} - {upper_bound:.3f}',
                'total_cases_in_range': len(median_cases),
                'sampled_cases': len(detailed_cases)
            },
            'business_interpretation': {
                'expected_behavior': '收费广场入口流量应与OD起点数量高度一致（比值接近1.0）',
                'actual_finding': f'中位数OD/流量比为{median_ratio:.1%}',
                'quality_assessment': 'abnormal' if median_ratio < 0.8 else 'normal',
                'recommended_actions': [
                    '检查流量数据采集是否存在重复计数问题',
                    '验证OD数据的完整性和时间匹配',
                    '分析数据关联逻辑是否正确',
                    '对比历史数据趋势，确定是否为系统性问题'
                ] if median_ratio < 0.8 else [
                    '数据质量良好，符合业务预期',
                    '继续监控数据一致性'
                ]
            }
        }

        logger.info(f"收费广场{analysis_type}中位数案例分析完成")
        return analysis_results

    def analyze_toll_square_balance(self):
        """收费广场进出流量平衡分析"""
        logger.info("开始收费广场进出流量平衡分析")

        # 获取入口流量统计
        onramp_stats = self._get_toll_square_flow_stats(self.onramp_flow_data, 'onramp')

        # 获取出口流量统计
        offramp_stats = self._get_toll_square_flow_stats(self.offramp_flow_data, 'offramp')

        # 合并进出流量数据
        balance_data = pd.merge(
            onramp_stats, offramp_stats,
            on=['square_code', 'date', 'hour'],
            how='outer'
        )

        # 填充缺失值
        balance_data['onramp_flow'] = balance_data['onramp_flow'].fillna(0)
        balance_data['offramp_flow'] = balance_data['offramp_flow'].fillna(0)

        # 计算平衡指标
        balance_data['flow_balance_ratio'] = balance_data['offramp_flow'] / (balance_data['onramp_flow'] + 1e-6)
        balance_data['flow_difference'] = balance_data['onramp_flow'] - balance_data['offramp_flow']
        balance_data['total_flow'] = balance_data['onramp_flow'] + balance_data['offramp_flow']

        # 统计分析
        balance_analysis = {
            'total_records': len(balance_data),
            'unique_squares': balance_data['square_code'].nunique(),
            'balance_ratio_stats': {
                'mean': balance_data['flow_balance_ratio'].mean(),
                'median': balance_data['flow_balance_ratio'].median(),
                'std': balance_data['flow_balance_ratio'].std(),
                'min': balance_data['flow_balance_ratio'].min(),
                'max': balance_data['flow_balance_ratio'].max(),
                'q25': balance_data['flow_balance_ratio'].quantile(0.25),
                'q75': balance_data['flow_balance_ratio'].quantile(0.75)
            },
            'imbalanced_squares': self._identify_imbalanced_squares(balance_data)
        }

        logger.info("收费广场进出流量平衡分析完成")
        return balance_analysis

    def _get_toll_square_flow_stats(self, flow_data, flow_type):
        """获取收费广场流量统计"""
        flow_stats = flow_data.copy()
        flow_stats['flow_date'] = pd.to_datetime(flow_stats['start_time']).dt.date
        flow_stats['flow_hour'] = pd.to_datetime(flow_stats['start_time']).dt.hour

        aggregated = flow_stats.groupby(['square_code', 'flow_date', 'flow_hour']).agg({
            'total': 'sum'
        }).reset_index()
        aggregated.columns = ['square_code', 'date', 'hour', f'{flow_type}_flow']

        return aggregated

    def _identify_imbalanced_squares(self, balance_data):
        """识别流量不平衡的收费广场"""
        # 按收费广场分组统计
        square_summary = balance_data.groupby('square_code').agg({
            'flow_balance_ratio': 'mean',
            'onramp_flow': 'sum',
            'offramp_flow': 'sum',
            'total_flow': 'sum'
        }).reset_index()

        # 识别不平衡的收费广场（比值偏离1较多）
        square_summary['balance_deviation'] = abs(square_summary['flow_balance_ratio'] - 1)
        imbalanced = square_summary[square_summary['balance_deviation'] > 0.3].copy()  # 偏差超过30%

        return {
            'count': len(imbalanced),
            'details': imbalanced.to_dict('records')
        }

    def generate_detailed_report(self, gantry_results, toll_square_results, output_file):
        """生成详细关联分析报告"""
        logger.info("生成详细关联分析报告")

        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>详细起始点关联分析报告</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .header { background-color: #f0f0f0; padding: 20px; border-radius: 5px; }
                .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
                table { border-collapse: collapse; width: 100%; margin: 10px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .metric { display: inline-block; margin: 10px; padding: 10px; background-color: #e8f4fd; border-radius: 5px; }
                .highlight { background-color: #fff3cd; }
                .code { background-color: #f8f9fa; padding: 10px; border-radius: 3px; font-family: monospace; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>详细起始点关联分析报告</h1>
                <p>生成时间: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
            </div>
        """

        # 门架关联分析部分
        if gantry_results:
            html_content += self._generate_gantry_section(gantry_results)

        # 收费广场关联分析部分
        if toll_square_results:
            html_content += self._generate_toll_square_section(toll_square_results)

        # 对比分析部分
        if gantry_results and toll_square_results:
            html_content += self._generate_comparison_section(gantry_results, toll_square_results)

        html_content += """
        </body>
        </html>
        """

        # 保存报告
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"详细关联分析报告已生成: {output_file}")

    def _generate_gantry_section(self, results):
        """生成门架分析部分"""
        html = """
        <div class="section">
            <h2>1. 门架起点关联分析</h2>
        """

        # 基本统计
        stats = results
        html += f"""
            <h3>基本统计信息</h3>
            <div class="metric">关联记录数: <strong>{stats['total_records']:,}</strong></div>
            <div class="metric">涉及门架数: <strong>{stats['unique_locations']}</strong></div>
            <div class="metric">相关系数: <strong>{stats['correlation_coefficient']:.3f}</strong></div>
            <div class="metric">平均OD/流量比: <strong>{stats['od_flow_ratio_stats']['mean']:.3f}</strong></div>
            <div class="metric">中位数OD/流量比: <strong>{stats['od_flow_ratio_stats']['median']:.3f}</strong></div>
        """

        # OD/流量比分布
        ratio_stats = stats['od_flow_ratio_stats']
        html += f"""
            <h3>OD/流量比分布</h3>
            <table>
                <tr><th>统计量</th><th>数值</th><th>说明</th></tr>
                <tr><td>最小值</td><td>{ratio_stats['min']:.4f}</td><td>最低的OD/流量比</td></tr>
                <tr><td>25%分位数</td><td>{ratio_stats['q25']:.4f}</td><td>25%的记录低于此值</td></tr>
                <tr><td>中位数</td><td>{ratio_stats['median']:.4f}</td><td>50%的记录低于此值</td></tr>
                <tr><td>75%分位数</td><td>{ratio_stats['q75']:.4f}</td><td>75%的记录低于此值</td></tr>
                <tr><td>最大值</td><td>{ratio_stats['max']:.4f}</td><td>最高的OD/流量比</td></tr>
                <tr><td>标准差</td><td>{ratio_stats['std']:.4f}</td><td>数据离散程度</td></tr>
            </table>
        """

        # 车型对比
        vehicle_stats = stats['vehicle_ratio_comparison']
        html += f"""
            <h3>车型结构对比</h3>
            <table>
                <tr><th>车型</th><th>OD数据占比</th><th>流量数据占比</th><th>差异</th><th>差异标准差</th></tr>
                <tr>
                    <td>货车</td>
                    <td>{vehicle_stats['od_truck_ratio_mean']:.1%}</td>
                    <td>{vehicle_stats['flow_truck_ratio_mean']:.1%}</td>
                    <td>{vehicle_stats['truck_ratio_diff_mean']:+.1%}</td>
                    <td>{vehicle_stats['truck_ratio_diff_std']:.3f}</td>
                </tr>
                <tr>
                    <td>客车</td>
                    <td>{vehicle_stats['od_passenger_ratio_mean']:.1%}</td>
                    <td>{vehicle_stats['flow_passenger_ratio_mean']:.1%}</td>
                    <td>{vehicle_stats['passenger_ratio_diff_mean']:+.1%}</td>
                    <td>{vehicle_stats['passenger_ratio_diff_std']:.3f}</td>
                </tr>
            </table>
        """



        # 时间模式
        if 'time_patterns' in results:
            html += self._generate_time_patterns_table(results['time_patterns'])

        html += "</div>"
        return html

    def _generate_toll_square_section(self, results):
        """生成收费广场分析部分"""
        html = """
        <div class="section">
            <h2>2. 收费广场入口关联分析</h2>
            <div class="alert">
                <strong>业务逻辑：</strong>收费广场入口流量应与以该收费广场为起点的OD数量高度一致（接近1:1关系）
            </div>
        """

        # 基本统计
        stats = results
        html += f"""
            <h3>基本统计信息</h3>
            <div class="metric">关联记录数: <strong>{stats['total_records']:,}</strong></div>
            <div class="metric">涉及收费广场数: <strong>{stats['unique_locations']}</strong></div>
            <div class="metric">相关系数: <strong>{stats['correlation_coefficient']:.3f}</strong></div>
        """

        # 数据质量评估（如果存在）
        if 'data_quality_assessment' in stats:
            quality = stats['data_quality_assessment']
            html += f"""
                <h3>数据质量评估</h3>
                <div class="alert alert-{'success' if quality['normal_ratio'] > 0.8 else 'warning'}">
                    <strong>预期：</strong>{quality['expected_consistency']}<br>
                    <strong>实际：</strong>{quality['actual_consistency']}
                </div>
                <div class="metric">正常记录: <strong>{quality['normal_records']:,}</strong> ({quality['normal_ratio']:.1%})</div>
                <div class="metric">异常记录: <strong>{quality['abnormal_records']:,}</strong> ({1-quality['normal_ratio']:.1%})</div>
            """

        # 流量/OD比分布（更符合业务逻辑）
        if 'flow_od_ratio_stats' in stats:
            ratio_stats = stats['flow_od_ratio_stats']
            html += f"""
                <h3>流量/OD比分布（核心指标）</h3>
                <div class="alert alert-info">
                    <strong>说明：</strong>此比值应接近1.0，表示流量数据与OD数据高度一致
                </div>
                <table>
                    <tr><th>统计量</th><th>数值</th><th>评估</th></tr>
                    <tr><td>平均值</td><td>{ratio_stats['mean']:.3f}</td><td>{'✓ 正常' if 0.8 <= ratio_stats['mean'] <= 1.2 else '⚠ 异常'}</td></tr>
                    <tr><td>中位数</td><td>{ratio_stats['median']:.3f}</td><td>{'✓ 正常' if 0.8 <= ratio_stats['median'] <= 1.2 else '⚠ 异常'}</td></tr>
                    <tr><td>标准差</td><td>{ratio_stats['std']:.3f}</td><td>{'✓ 稳定' if ratio_stats['std'] < 0.3 else '⚠ 波动大'}</td></tr>
                    <tr><td>最小值</td><td>{ratio_stats['min']:.3f}</td><td></td></tr>
                    <tr><td>最大值</td><td>{ratio_stats['max']:.3f}</td><td></td></tr>
                </table>
            """

        # OD/流量比分布（传统指标，但重新解释）
        if 'od_flow_ratio_stats' in stats:
            od_ratio_stats = stats['od_flow_ratio_stats']
            html += f"""
                <h3>OD/流量比分布（辅助指标）</h3>
                <div class="alert alert-warning">
                    <strong>重要提醒：</strong>如果此比值约为50%，说明流量数据是OD数据的2倍，存在数据质量问题需要调查
                </div>
                <table>
                    <tr><th>统计量</th><th>数值</th><th>问题评估</th></tr>
                    <tr><td>平均值</td><td>{od_ratio_stats['mean']:.3f}</td><td>{'🔴 数据异常' if od_ratio_stats['mean'] < 0.8 else '✓ 正常'}</td></tr>
                    <tr><td>中位数</td><td>{od_ratio_stats['median']:.3f}</td><td>{'🔴 数据异常' if od_ratio_stats['median'] < 0.8 else '✓ 正常'}</td></tr>
                    <tr><td>25%分位数</td><td>{od_ratio_stats['q25']:.4f}</td><td></td></tr>
                    <tr><td>75%分位数</td><td>{od_ratio_stats['q75']:.4f}</td><td></td></tr>
                </table>
            """

        # 数据质量问题分析
        if 'od_flow_ratio_stats' in stats and stats['od_flow_ratio_stats']['mean'] < 0.8:
            html += f"""
                <h3>🔍 数据质量问题分析</h3>
                <div class="alert alert-danger">
                    <strong>发现问题：</strong>平均OD/流量比为{stats['od_flow_ratio_stats']['mean']:.1%}，远低于预期的80%-100%
                    <br><strong>可能原因：</strong>
                    <ul>
                        <li>流量数据存在重复计算</li>
                        <li>OD数据存在缺失或采样不完整</li>
                        <li>数据时间范围不匹配</li>
                        <li>数据关联逻辑有误</li>
                    </ul>
                    <br><strong>建议措施：</strong>
                    <ul>
                        <li>检查流量数据采集设备是否存在重复计数</li>
                        <li>验证OD数据的完整性和准确性</li>
                        <li>确认数据关联的时间和空间匹配逻辑</li>
                    </ul>
                </div>
            """

        # 车型对比（如果存在）
        if 'vehicle_ratio_comparison' in stats:
            vehicle_stats = stats['vehicle_ratio_comparison']
            html += f"""
                <h3>车型结构对比</h3>
                <table>
                    <tr><th>车型</th><th>OD数据占比</th><th>流量数据占比</th><th>差异</th><th>差异标准差</th></tr>
                    <tr>
                        <td>货车</td>
                        <td>{vehicle_stats['od_truck_ratio_mean']:.1%}</td>
                        <td>{vehicle_stats['flow_truck_ratio_mean']:.1%}</td>
                        <td>{vehicle_stats['truck_ratio_diff_mean']:+.1%}</td>
                        <td>{vehicle_stats['truck_ratio_diff_std']:.3f}</td>
                    </tr>
                    <tr>
                        <td>客车</td>
                        <td>{vehicle_stats['od_passenger_ratio_mean']:.1%}</td>
                        <td>{vehicle_stats['flow_passenger_ratio_mean']:.1%}</td>
                        <td>{vehicle_stats['passenger_ratio_diff_mean']:+.1%}</td>
                        <td>{vehicle_stats['passenger_ratio_diff_std']:.3f}</td>
                    </tr>
                </table>
            """

        # 时间模式
        if 'time_patterns' in results:
            html += self._generate_time_patterns_table(results['time_patterns'])

        html += "</div>"
        return html

    def _generate_time_patterns_table(self, patterns):
        """生成时间模式表格"""
        html = "<h3>时间模式分析</h3>"

        if 'peak_analysis' in patterns:
            peak_info = patterns['peak_analysis']
            html += f"""
            <h4>高峰时段分析</h4>
            <ul>
                <li>高峰时段: {', '.join(map(str, peak_info['peak_hours']))}点</li>
                <li>低峰时段: {', '.join(map(str, peak_info['off_peak_hours']))}点</li>
                <li>高峰期平均OD/流量比: {peak_info['peak_avg_od_flow_ratio']:.3f}</li>
                <li>低峰期平均OD/流量比: {peak_info['off_peak_avg_od_flow_ratio']:.3f}</li>
            </ul>
            """

        return html

    def _generate_comparison_section(self, gantry_results, toll_square_results):
        """生成对比分析部分"""
        html = """
        <div class="section">
            <h2>3. 门架 vs 收费广场对比分析</h2>
        """

        # 基本对比
        html += f"""
            <table>
                <tr><th>指标</th><th>门架起点</th><th>收费广场起点</th><th>差异</th></tr>
                <tr>
                    <td>关联记录数</td>
                    <td>{gantry_results['total_records']:,}</td>
                    <td>{toll_square_results['total_records']:,}</td>
                    <td>{toll_square_results['total_records'] - gantry_results['total_records']:+,}</td>
                </tr>
                <tr>
                    <td>相关系数</td>
                    <td>{gantry_results['correlation_coefficient']:.3f}</td>
                    <td>{toll_square_results['correlation_coefficient']:.3f}</td>
                    <td>{toll_square_results['correlation_coefficient'] - gantry_results['correlation_coefficient']:+.3f}</td>
                </tr>
                <tr>
                    <td>平均OD/流量比</td>
                    <td>{gantry_results['od_flow_ratio_stats']['mean']:.3f}</td>
                    <td>{toll_square_results['od_flow_ratio_stats']['mean']:.3f}</td>
                    <td>{toll_square_results['od_flow_ratio_stats']['mean'] - gantry_results['od_flow_ratio_stats']['mean']:+.3f}</td>
                </tr>
            </table>
        """

        html += "</div>"
        return html

    def export_detailed_data(self):
        """导出详细分析数据"""
        output_dir = os.path.join(os.path.dirname(__file__), 'detailed_correlation_output')
        os.makedirs(output_dir, exist_ok=True)

        # 导出各类关联分析数据
        if hasattr(self, 'gantry_results'):
            gantry_file = os.path.join(output_dir, 'detailed_gantry_correlation.json')
            self._save_json_results(self.gantry_results, gantry_file)
            logger.info(f"门架关联分析数据已导出: {gantry_file}")

        if hasattr(self, 'toll_square_results'):
            toll_square_file = os.path.join(output_dir, 'detailed_toll_square_correlation.json')
            self._save_json_results(self.toll_square_results, toll_square_file)
            logger.info(f"收费广场关联分析数据已导出: {toll_square_file}")

    def _save_json_results(self, data, filepath):
        """保存JSON格式的结果"""
        import json
        
        # 处理不能序列化的数据类型
        def default_serializer(obj):
            if hasattr(obj, 'isoformat'):  # datetime objects
                return obj.isoformat()
            elif hasattr(obj, 'item'):  # numpy scalars
                return obj.item()
            elif str(type(obj)).startswith('<class \'numpy.'):
                return str(obj)
            else:
                return str(obj)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=default_serializer)
        except Exception as e:
            logger.warning(f"保存JSON文件失败: {e}")

def parse_datetime(date_str):
    """解析日期时间字符串，支持多种格式"""
    from datetime import datetime

    # 尝试不同的日期时间格式
    formats = [
        '%Y-%m-%d %H:%M:%S',  # 完整日期时间格式
        '%Y-%m-%d %H:%M',     # 日期+小时分钟
        '%Y-%m-%d %H',        # 日期+小时
        '%Y-%m-%d',           # 仅日期
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    raise ValueError(f"无法解析日期时间格式: {date_str}。支持的格式: YYYY-MM-DD, YYYY-MM-DD HH, YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='详细起始点关联分析')
    parser.add_argument('--start-date', required=True, help='开始日期时间（格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）')
    parser.add_argument('--end-date', required=True, help='结束日期时间（格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）')
    parser.add_argument('--output-dir', default='detailed_correlation_output', help='输出目录')

    args = parser.parse_args()

    # 解析和验证日期时间
    try:
        # 尝试解析完整的日期时间格式
        if ' ' in args.start_date:
            start_dt = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M:%S')
        else:
            start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
            
        if ' ' in args.end_date:
            end_dt = datetime.strptime(args.end_date, '%Y-%m-%d %H:%M:%S')
        else:
            end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')

        if start_dt >= end_dt:
            print("❌ 错误: 开始时间必须早于结束时间")
            return 1

        # 转换为字符串格式用于SQL查询
        start_date_sql = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_date_sql = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # 计算时间范围
        time_diff = end_dt - start_dt
        print(f"时间范围: {time_diff}")

    except ValueError as e:
        print(f"❌ 日期时间格式错误: {e}")
        return 1

    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 80)
    print("详细起始点关联分析工具")
    print("=" * 80)
    print(f"分析参数:")
    print(f"  开始时间: {args.start_date}")
    print(f"  结束时间: {args.end_date}")
    print(f"  数据模式: 全量数据（通过时间范围控制）")
    print(f"  输出目录: {args.output_dir}")
    print()

    try:
        # 验证配置
        if not validate_config():
            return 1

        # 初始化分析器
        analyzer = DetailedCorrelationAnalyzer()

        # 连接数据库
        analyzer.connect_database()

        # 加载数据
        print("1. 加载数据...")
        analyzer.load_data(start_date_sql, end_date_sql)

        # 门架关联分析
        print("2. 执行门架关联分析...")
        print("   2.1 门架起点分析...")
        gantry_origin_results = analyzer.analyze_gantry_correlation_detailed()

        print("   2.2 门架终点分析...")
        gantry_destination_results = analyzer.analyze_gantry_destination_correlation()

        print("   2.3 门架途中流量分析...")
        gantry_transit_results = analyzer.analyze_gantry_transit_flow()

        # 收费广场关联分析
        print("3. 执行收费广场关联分析...")
        print("   3.1 收费广场入口分析...")
        toll_square_entry_results = analyzer.analyze_toll_square_correlation_detailed()

        print("   3.2 收费广场出口分析...")
        toll_square_exit_results = analyzer.analyze_toll_square_exit_correlation()

        print("   3.3 收费广场流量平衡分析...")
        toll_square_balance_results = analyzer.analyze_toll_square_balance()

        print("   3.4 收费广场中位数案例分析...")
        toll_square_median_entry = analyzer.analyze_median_ratio_cases('entry')
        toll_square_median_exit = analyzer.analyze_median_ratio_cases('exit')

        # 生成报告
        print("4. 生成详细报告...")
        report_file = os.path.join(args.output_dir, "detailed_correlation_report.html")
        analyzer.generate_enhanced_report({
            'gantry_origin': gantry_origin_results,
            'gantry_destination': gantry_destination_results,
            'gantry_transit': gantry_transit_results,
            'toll_square_entry': toll_square_entry_results,
            'toll_square_exit': toll_square_exit_results,
            'toll_square_balance': toll_square_balance_results,
            'toll_square_median_entry': toll_square_median_entry,
            'toll_square_median_exit': toll_square_median_exit
        }, report_file)

        # 导出详细数据
        print("5. 导出详细数据...")
        analyzer.export_enhanced_data({
            'gantry_origin': gantry_origin_results,
            'gantry_destination': gantry_destination_results,
            'gantry_transit': gantry_transit_results,
            'toll_square_entry': toll_square_entry_results,
            'toll_square_exit': toll_square_exit_results,
            'toll_square_balance': toll_square_balance_results,
            'toll_square_median_entry': toll_square_median_entry,
            'toll_square_median_exit': toll_square_median_exit
        }, args.output_dir)

        print("=" * 80)
        print("详细关联分析完成！")
        print("=" * 80)

        # 输出关键结果
        if gantry_origin_results:
            print(f"📊 门架起点关联分析:")
            print(f"   关联记录数: {gantry_origin_results['total_records']:,}")
            print(f"   涉及门架数: {gantry_origin_results['unique_locations']}")
            print(f"   相关系数: {gantry_origin_results['correlation_coefficient']:.3f}")
            print(f"   平均OD/流量比: {gantry_origin_results['od_flow_ratio_stats']['mean']:.3f}")

        if gantry_transit_results:
            print(f"📊 门架途中流量分析:")
            print(f"   涉及门架数: {gantry_transit_results['unique_gantries']}")
            print(f"   平均途中流量占比: {gantry_transit_results['avg_transit_ratio']:.1%}")
            print(f"   平均OD占比: {gantry_transit_results['avg_od_ratio']:.1%}")

        if toll_square_entry_results:
            print(f"📊 收费广场入口关联分析:")
            print(f"   关联记录数: {toll_square_entry_results['total_records']:,}")
            print(f"   涉及收费广场数: {toll_square_entry_results['unique_locations']}")
            print(f"   相关系数: {toll_square_entry_results['correlation_coefficient']:.3f}")
            print(f"   平均OD/流量比: {toll_square_entry_results['od_flow_ratio_stats']['mean']:.3f}")

        if toll_square_balance_results:
            print(f"📊 收费广场流量平衡分析:")
            print(f"   涉及收费广场数: {toll_square_balance_results['unique_squares']}")
            print(f"   中位数进出流量比: {toll_square_balance_results['balance_ratio_stats']['median']:.3f}")
            print(f"   不平衡收费广场数: {toll_square_balance_results['imbalanced_squares']['count']}")

        print(f"\n📁 结果保存在: {args.output_dir}")
        print(f"📄 查看报告: {report_file}")

    except Exception as e:
        logger.error(f"分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
