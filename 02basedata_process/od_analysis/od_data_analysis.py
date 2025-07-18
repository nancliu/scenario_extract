#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OD事实表数据分析脚本
用于分析OD数据特征，为OD重要性分级提供数据基础

功能：
1. OD数据基本特征分析
2. OD对数量和分布分析
3. OD数据异常检测
4. 门架流量与收费广场流量一致性分析
5. 生成数据质量报告

作者：数据分析团队
日期：2025-01-17
版本：v1.0
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
import psycopg2
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 添加config目录到Python路径
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config'))

# 设置中文字体和警告过滤
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('od_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ODDataAnalyzer:
    """OD数据分析器"""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        初始化分析器
        
        Args:
            db_config: 数据库连接配置
        """
        self.db_config = db_config
        self.engine = None
        self.od_data = None
        self.gantry_flow_data = None
        self.toll_flow_data = None
        
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
    
    def load_od_data(self, start_date: str, end_date: str, limit: Optional[int] = None):
        """
        加载OD数据 - 优化版本

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            limit: 限制记录数，用于测试
        """
        logger.info(f"加载OD数据: {start_date} 到 {end_date}")

        # 首先检查数据量
        count_sql = f"""
        SELECT COUNT(*) as total_count
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        """

        try:
            count_result = pd.read_sql(count_sql, self.engine)
            total_count = count_result['total_count'].iloc[0]
            logger.info(f"查询时间范围内总记录数: {total_count:,}")

            # 如果数据量很大且没有设置limit，给出警告
            if total_count > 1000000 and limit is None:
                logger.warning(f"数据量较大({total_count:,}条)，建议使用--limit参数限制记录数")
                logger.warning("或者使用--quick模式进行快速测试")

        except Exception as e:
            logger.warning(f"无法获取记录数统计: {e}")
            total_count = 0

        # 构建优化的查询
        limit_clause = f"LIMIT {limit}" if limit else ""

        # 优化1: 移除ORDER BY以提高性能（如果不需要严格排序）
        # 优化2: 只选择必要的字段
        sql = f"""
        SELECT
            pass_id,
            vehicle_type,
            start_time,
            start_square_code,
            start_square_name,
            start_station_code,
            start_station_name,
            end_time,
            end_square_code,
            end_square_name,
            end_station_code,
            end_station_name,
            direction,
            batch_id
        FROM dwd.dwd_od_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        {limit_clause}
        """

        try:
            start_time = pd.Timestamp.now()
            logger.info("开始执行SQL查询...")

            # 使用chunksize分批读取大数据集
            if limit is None and total_count > 500000:
                logger.info("数据量较大，使用分批读取模式...")
                chunks = []
                chunk_size = 100000

                for chunk in pd.read_sql(sql, self.engine, chunksize=chunk_size):
                    chunks.append(chunk)
                    logger.info(f"已读取 {len(chunks) * chunk_size} 条记录...")

                self.od_data = pd.concat(chunks, ignore_index=True)
            else:
                self.od_data = pd.read_sql(sql, self.engine)

            end_time = pd.Timestamp.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"成功加载OD数据: {len(self.od_data):,} 条记录")
            logger.info(f"查询耗时: {duration:.2f} 秒")

            if len(self.od_data) > 0:
                logger.info(f"数据时间范围: {self.od_data['start_time'].min()} 到 {self.od_data['start_time'].max()}")

        except Exception as e:
            logger.error(f"加载OD数据失败: {e}")
            raise
    
    def load_flow_data(self, start_date: str, end_date: str):
        """
        加载门架流量和收费广场流量数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info("加载门架流量数据")
        
        # 加载门架流量数据
        gantry_sql = f"""
        SELECT 
            start_gantryid,
            end_gantryid,
            gantry_name,
            start_time,
            total,
            total_k,
            total_h,
            total_t
        FROM dwd.dwd_flow_gantry_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        """
        
        # 加载收费广场流量数据（上匝道+下匝道）
        onramp_sql = f"""
        SELECT 
            station_code,
            station_name,
            square_code,
            square_name,
            start_time,
            total,
            total_k,
            total_h,
            total_t,
            'onramp' as flow_type
        FROM dwd.dwd_flow_onramp_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        """
        
        offramp_sql = f"""
        SELECT 
            station_code,
            station_name,
            square_code,
            square_name,
            start_time,
            total,
            total_k,
            total_h,
            total_t,
            'offramp' as flow_type
        FROM dwd.dwd_flow_offramp_weekly
        WHERE start_time >= '{start_date}'
          AND start_time < '{end_date}'
        """
        
        try:
            self.gantry_flow_data = pd.read_sql(gantry_sql, self.engine)
            onramp_data = pd.read_sql(onramp_sql, self.engine)
            offramp_data = pd.read_sql(offramp_sql, self.engine)
            self.toll_flow_data = pd.concat([onramp_data, offramp_data], ignore_index=True)
            
            logger.info(f"成功加载门架流量数据: {len(self.gantry_flow_data)} 条记录")
            logger.info(f"成功加载收费广场流量数据: {len(self.toll_flow_data)} 条记录")
        except Exception as e:
            logger.error(f"加载流量数据失败: {e}")
            raise
    
    def analyze_od_basic_stats(self) -> Dict:
        """分析OD数据基本统计信息"""
        logger.info("开始OD数据基本统计分析")
        
        if self.od_data is None:
            raise ValueError("OD数据未加载")
        
        stats = {}
        
        # 基本统计
        stats['total_records'] = len(self.od_data)
        stats['unique_pass_ids'] = self.od_data['pass_id'].nunique()
        stats['date_range'] = {
            'start': self.od_data['start_time'].min(),
            'end': self.od_data['start_time'].max()
        }
        
        # 车辆类型分布
        stats['vehicle_type_dist'] = self.od_data['vehicle_type'].value_counts().to_dict()



        # 方向分布
        stats['direction_dist'] = self.od_data['direction'].value_counts().to_dict()
        
        # OD对统计 - 当square_code为空时使用station_code（门架编号）
        self.od_data['start_point_code'] = self.od_data['start_square_code'].fillna(self.od_data['start_station_code'])
        self.od_data['end_point_code'] = self.od_data['end_square_code'].fillna(self.od_data['end_station_code'])

        self.od_data['od_pair'] = (
            self.od_data['start_point_code'].astype(str) + '-' +
            self.od_data['end_point_code'].astype(str)
        )

        # 标记OD对的类型（收费广场对收费广场 vs 门架对门架 vs 混合）
        self.od_data['start_point_type'] = self.od_data['start_square_code'].apply(
            lambda x: 'toll_square' if pd.notna(x) else 'gantry'
        )
        self.od_data['end_point_type'] = self.od_data['end_square_code'].apply(
            lambda x: 'toll_square' if pd.notna(x) else 'gantry'
        )
        self.od_data['od_pair_type'] = (
            self.od_data['start_point_type'] + '_to_' + self.od_data['end_point_type']
        )

        # 标记同起终点记录（穿过车辆）
        self.od_data['is_pass_through'] = (
            self.od_data['start_point_code'] == self.od_data['end_point_code']
        )

        # 创建有效OD数据（排除同起终点）
        self.valid_od_data = self.od_data[~self.od_data['is_pass_through']].copy()
        logger.info(f"有效OD数据: {len(self.valid_od_data):,} 条记录 (排除 {len(self.od_data) - len(self.valid_od_data):,} 条穿过车辆)")

        # 为有效OD数据添加时间字段用于关联分析
        self.valid_od_data['start_hour'] = pd.to_datetime(self.valid_od_data['start_time']).dt.hour
        self.valid_od_data['start_date'] = pd.to_datetime(self.valid_od_data['start_time']).dt.date

        # 车型分类统计（基于有效OD数据，排除穿过车辆）
        def classify_vehicle_type(vehicle_type):
            if vehicle_type.startswith('k'):
                return 'passenger'  # 客车
            elif vehicle_type.startswith('h'):
                return 'truck'      # 货车
            elif vehicle_type.startswith('t'):
                return 'trailer'    # 挂车
            else:
                return 'other'

        self.valid_od_data['vehicle_class'] = self.valid_od_data['vehicle_type'].apply(classify_vehicle_type)
        vehicle_class_counts = self.valid_od_data['vehicle_class'].value_counts()
        total_valid = len(self.valid_od_data)

        stats['vehicle_class_distribution'] = {
            'passenger_count': vehicle_class_counts.get('passenger', 0),
            'truck_count': vehicle_class_counts.get('truck', 0),
            'trailer_count': vehicle_class_counts.get('trailer', 0),
            'other_count': vehicle_class_counts.get('other', 0),
            'passenger_ratio': vehicle_class_counts.get('passenger', 0) / total_valid if total_valid > 0 else 0,
            'truck_ratio': vehicle_class_counts.get('truck', 0) / total_valid if total_valid > 0 else 0,
            'trailer_ratio': vehicle_class_counts.get('trailer', 0) / total_valid if total_valid > 0 else 0,
            'other_ratio': vehicle_class_counts.get('other', 0) / total_valid if total_valid > 0 else 0
        }
        # 唯一OD对数量（基于有效OD数据，排除穿过车辆）
        stats['unique_od_pairs'] = self.valid_od_data['od_pair'].nunique()
        stats['total_pass_through'] = len(self.od_data) - len(self.valid_od_data)

        # TOP 20 OD对（基于有效OD数据）
        stats['top_od_pairs'] = self.valid_od_data['od_pair'].value_counts().head(20).to_dict()

        # OD对类型统计
        stats['od_pair_type_dist'] = self.od_data['od_pair_type'].value_counts().to_dict()
        stats['start_point_type_dist'] = self.od_data['start_point_type'].value_counts().to_dict()
        stats['end_point_type_dist'] = self.od_data['end_point_type'].value_counts().to_dict()
        
        # 收费站统计
        stats['unique_start_stations'] = self.od_data['start_station_code'].nunique()
        stats['unique_end_stations'] = self.od_data['end_station_code'].nunique()
        stats['unique_start_squares'] = self.od_data['start_square_code'].nunique()
        stats['unique_end_squares'] = self.od_data['end_square_code'].nunique()
        
        # 时间分布
        self.od_data['hour'] = pd.to_datetime(self.od_data['start_time']).dt.hour
        self.od_data['weekday'] = pd.to_datetime(self.od_data['start_time']).dt.weekday
        stats['hourly_dist'] = self.od_data['hour'].value_counts().sort_index().to_dict()
        stats['weekday_dist'] = self.od_data['weekday'].value_counts().sort_index().to_dict()
        
        logger.info("OD数据基本统计分析完成")
        return stats
    
    def detect_od_anomalies(self) -> Dict:
        """检测OD数据异常"""
        logger.info("开始OD数据异常检测")

        anomalies = {}
        anomaly_samples = {}  # 存储异常样例

        # 1. 关键字段空值检测（只检测真正的异常）
        # square_code和square_name为空是正常的（门架点位），只有同时缺失station_code才异常
        missing_both_start = self.od_data[
            self.od_data['start_square_code'].isna() & self.od_data['start_station_code'].isna()
        ]
        missing_both_end = self.od_data[
            self.od_data['end_square_code'].isna() & self.od_data['end_station_code'].isna()
        ]
        anomalies['missing_start_codes'] = len(missing_both_start)
        anomalies['missing_end_codes'] = len(missing_both_end)

        # 保存样例
        if len(missing_both_start) > 0:
            anomaly_samples['missing_start_codes'] = missing_both_start.head(5)[
                ['pass_id', 'start_time', 'start_square_code', 'start_station_code']
            ].to_dict('records')
        if len(missing_both_end) > 0:
            anomaly_samples['missing_end_codes'] = missing_both_end.head(5)[
                ['pass_id', 'end_time', 'end_square_code', 'end_station_code']
            ].to_dict('records')

        # 检测关键业务字段的空值
        critical_null_fields = ['pass_id', 'start_time', 'end_time']
        critical_nulls = {}
        for field in critical_null_fields:
            null_records = self.od_data[self.od_data[field].isnull()]
            null_count = len(null_records)
            if null_count > 0:
                critical_nulls[field] = null_count
                anomaly_samples[f'critical_null_{field}'] = null_records.head(5)[
                    ['pass_id', 'start_time', 'end_time', field]
                ].to_dict('records')
        if critical_nulls:
            anomalies['critical_null_values'] = critical_nulls

        # 2. 重复记录检测
        duplicate_records = self.od_data[self.od_data.duplicated(keep=False)]
        duplicate_count = len(duplicate_records)
        anomalies['duplicate_records'] = duplicate_count
        if duplicate_count > 0:
            anomaly_samples['duplicate_records'] = duplicate_records.head(10)[
                ['pass_id', 'start_time', 'start_point_code', 'end_point_code']
            ].to_dict('records')

        # 3. 时间异常检测
        # 检查开始时间晚于结束时间的记录
        time_anomaly = self.od_data[
            pd.to_datetime(self.od_data['start_time']) > pd.to_datetime(self.od_data['end_time'])
        ]
        anomalies['time_anomalies'] = len(time_anomaly)
        if len(time_anomaly) > 0:
            anomaly_samples['time_anomalies'] = time_anomaly.head(5)[
                ['pass_id', 'start_time', 'end_time', 'vehicle_type']
            ].to_dict('records')

        # 4. 同起点同终点检测（特殊情况：穿过目标高速公路的车辆）
        same_origin_dest = self.od_data[
            self.od_data['start_point_code'] == self.od_data['end_point_code']
        ]
        # 注意：这不是异常，而是特殊情况，仅作记录
        anomalies['pass_through_vehicles'] = len(same_origin_dest)
        if len(same_origin_dest) > 0:
            anomaly_samples['pass_through_vehicles'] = same_origin_dest.head(10)[
                ['pass_id', 'start_time', 'start_point_code', 'end_point_code', 'vehicle_type']
            ].to_dict('records')

        # 5. 异常长的通行时间检测（超过24小时）
        self.od_data['travel_time_hours'] = (
            pd.to_datetime(self.od_data['end_time']) -
            pd.to_datetime(self.od_data['start_time'])
        ).dt.total_seconds() / 3600

        long_travel = self.od_data[self.od_data['travel_time_hours'] > 24]
        anomalies['long_travel_time'] = len(long_travel)
        if len(long_travel) > 0:
            anomaly_samples['long_travel_time'] = long_travel.head(5)[
                ['pass_id', 'start_time', 'end_time', 'travel_time_hours', 'start_point_code', 'end_point_code']
            ].to_dict('records')

        # 6. 异常短的通行时间检测（少于1分钟，排除同起终点的穿过车辆）
        short_travel = self.od_data[
            (self.od_data['travel_time_hours'] < 1/60) &
            (self.od_data['start_point_code'] != self.od_data['end_point_code'])
        ]
        anomalies['short_travel_time'] = len(short_travel)
        if len(short_travel) > 0:
            anomaly_samples['short_travel_time'] = short_travel.head(10)[
                ['pass_id', 'start_time', 'end_time', 'travel_time_hours', 'start_point_code', 'end_point_code']
            ].to_dict('records')

        # 7. 数据完整性统计（用于信息展示，不算异常）
        info_stats = {}
        info_stats['start_square_null_count'] = self.od_data['start_square_code'].isnull().sum()
        info_stats['end_square_null_count'] = self.od_data['end_square_code'].isnull().sum()
        info_stats['start_square_null_ratio'] = info_stats['start_square_null_count'] / len(self.od_data)
        info_stats['end_square_null_ratio'] = info_stats['end_square_null_count'] / len(self.od_data)
        anomalies['data_info'] = info_stats

        # 保存异常样例
        anomalies['anomaly_samples'] = anomaly_samples

        logger.info("OD数据异常检测完成")
        return anomalies

    def analyze_origin_correlation(self) -> Dict:
        """分析起始点与对应流量数据的关联性"""
        logger.info("开始起始点关联分析")

        correlation_results = {}

        # 1. 按起始点类型分组分析
        gantry_origins = self.valid_od_data[self.valid_od_data['start_point_type'] == 'gantry']
        toll_square_origins = self.valid_od_data[self.valid_od_data['start_point_type'] == 'toll_square']

        logger.info(f"门架起点OD: {len(gantry_origins):,} 条")
        logger.info(f"收费广场起点OD: {len(toll_square_origins):,} 条")

        # 2. 门架起点关联分析
        if len(gantry_origins) > 0 and hasattr(self, 'gantry_flow_data'):
            gantry_correlation = self._analyze_gantry_correlation(gantry_origins)
            correlation_results['gantry_correlation'] = gantry_correlation

        # 3. 收费广场起点关联分析
        if len(toll_square_origins) > 0 and hasattr(self, 'onramp_flow_data'):
            toll_correlation = self._analyze_toll_square_correlation(toll_square_origins)
            correlation_results['toll_square_correlation'] = toll_correlation

        # 4. 综合统计
        correlation_results['summary'] = {
            'total_valid_od': len(self.valid_od_data),
            'gantry_origin_count': len(gantry_origins),
            'toll_square_origin_count': len(toll_square_origins),
            'gantry_origin_ratio': len(gantry_origins) / len(self.valid_od_data) if len(self.valid_od_data) > 0 else 0,
            'toll_square_origin_ratio': len(toll_square_origins) / len(self.valid_od_data) if len(self.valid_od_data) > 0 else 0
        }

        logger.info("起始点关联分析完成")
        return correlation_results

    def _analyze_gantry_correlation(self, gantry_origins: pd.DataFrame) -> Dict:
        """分析门架起点与门架流量的关联性"""
        logger.info("分析门架起点关联性")

        # 车型分类函数
        def classify_vehicle_type(vehicle_type):
            if vehicle_type.startswith('k'):
                return 'passenger'  # 客车
            elif vehicle_type.startswith('h'):
                return 'truck'      # 货车
            elif vehicle_type.startswith('t'):
                return 'trailer'    # 挂车
            else:
                return 'other'

        # 为OD数据添加车型分类
        gantry_origins_with_class = gantry_origins.copy()
        gantry_origins_with_class['vehicle_class'] = gantry_origins_with_class['vehicle_type'].apply(classify_vehicle_type)

        # 按门架编号和小时聚合OD数据，包含车型分析
        od_hourly = gantry_origins_with_class.groupby(['start_station_code', 'start_hour']).agg({
            'pass_id': 'count',
            'vehicle_type': lambda x: x.value_counts().to_dict(),
            'vehicle_class': lambda x: x.value_counts().to_dict()
        }).reset_index()
        od_hourly.columns = ['gantry_code', 'hour', 'od_count', 'od_vehicle_types', 'od_vehicle_classes']

        # 计算车型占比
        def calculate_vehicle_ratios(vehicle_classes):
            total = sum(vehicle_classes.values())
            if total == 0:
                return {'passenger_ratio': 0, 'truck_ratio': 0, 'trailer_ratio': 0}
            return {
                'passenger_ratio': vehicle_classes.get('passenger', 0) / total,
                'truck_ratio': vehicle_classes.get('truck', 0) / total,
                'trailer_ratio': vehicle_classes.get('trailer', 0) / total
            }

        od_hourly['od_vehicle_ratios'] = od_hourly['od_vehicle_classes'].apply(calculate_vehicle_ratios)

        # 准备门架流量数据用于关联
        if hasattr(self, 'gantry_flow_data') and not self.gantry_flow_data.empty:
            # 添加小时字段到门架流量数据
            gantry_flow = self.gantry_flow_data.copy()
            gantry_flow['hour'] = pd.to_datetime(gantry_flow['start_time']).dt.hour

            # 按门架和小时聚合流量数据
            flow_hourly = gantry_flow.groupby(['start_gantryid', 'hour']).agg({
                'total': 'sum',
                'total_k': 'sum',
                'total_h': 'sum',
                'total_t': 'sum'
            }).reset_index()
            flow_hourly.columns = ['gantry_code', 'hour', 'flow_total', 'flow_k', 'flow_h', 'flow_t']

            # 计算流量数据的车型占比
            flow_hourly['flow_passenger_ratio'] = flow_hourly['flow_k'] / flow_hourly['flow_total']
            flow_hourly['flow_truck_ratio'] = flow_hourly['flow_h'] / flow_hourly['flow_total']
            flow_hourly['flow_trailer_ratio'] = flow_hourly['flow_t'] / flow_hourly['flow_total']
            flow_hourly = flow_hourly.fillna(0)

            # 关联OD数据和流量数据
            correlation_data = pd.merge(
                od_hourly,
                flow_hourly,
                on=['gantry_code', 'hour'],
                how='inner'
            )

            if not correlation_data.empty:
                # 计算相关性
                correlation_stats = {
                    'matched_records': len(correlation_data),
                    'total_od_records': len(od_hourly),
                    'match_ratio': len(correlation_data) / len(od_hourly) if len(od_hourly) > 0 else 0,
                    'correlation_coefficient': correlation_data['od_count'].corr(correlation_data['flow_total']) if len(correlation_data) > 1 else None
                }

                # 流量对比分析
                correlation_data['od_flow_ratio'] = correlation_data['od_count'] / correlation_data['flow_total']
                correlation_data['od_flow_ratio'] = correlation_data['od_flow_ratio'].fillna(0)

                # 提取车型占比数据
                od_ratios_df = pd.json_normalize(correlation_data['od_vehicle_ratios'])
                correlation_data = pd.concat([correlation_data, od_ratios_df], axis=1)

                # 车型占比对比分析
                correlation_data['truck_ratio_diff'] = correlation_data['truck_ratio'] - correlation_data['flow_truck_ratio']
                correlation_data['passenger_ratio_diff'] = correlation_data['passenger_ratio'] - correlation_data['flow_passenger_ratio']

                # 统计信息
                correlation_stats.update({
                    'avg_od_flow_ratio': correlation_data['od_flow_ratio'].mean(),
                    'median_od_flow_ratio': correlation_data['od_flow_ratio'].median(),
                    'max_od_flow_ratio': correlation_data['od_flow_ratio'].max(),
                    'min_od_flow_ratio': correlation_data['od_flow_ratio'].min(),
                    # 车型占比统计
                    'avg_od_truck_ratio': correlation_data['truck_ratio'].mean(),
                    'avg_flow_truck_ratio': correlation_data['flow_truck_ratio'].mean(),
                    'avg_truck_ratio_diff': correlation_data['truck_ratio_diff'].mean(),
                    'avg_od_passenger_ratio': correlation_data['passenger_ratio'].mean(),
                    'avg_flow_passenger_ratio': correlation_data['flow_passenger_ratio'].mean(),
                    'avg_passenger_ratio_diff': correlation_data['passenger_ratio_diff'].mean()
                })

                # 保存详细数据样例
                sample_columns = ['gantry_code', 'hour', 'od_count', 'flow_total', 'od_flow_ratio',
                                'truck_ratio', 'flow_truck_ratio', 'truck_ratio_diff',
                                'passenger_ratio', 'flow_passenger_ratio', 'passenger_ratio_diff']
                correlation_stats['sample_data'] = correlation_data[sample_columns].head(10).to_dict('records')

                return correlation_stats

        return {'error': '无法进行门架关联分析，缺少流量数据'}

    def _analyze_toll_square_correlation(self, toll_square_origins: pd.DataFrame) -> Dict:
        """分析收费广场起点与收费广场流量的关联性"""
        logger.info("分析收费广场起点关联性")

        # 车型分类函数
        def classify_vehicle_type(vehicle_type):
            if vehicle_type.startswith('k'):
                return 'passenger'  # 客车
            elif vehicle_type.startswith('h'):
                return 'truck'      # 货车
            elif vehicle_type.startswith('t'):
                return 'trailer'    # 挂车
            else:
                return 'other'

        # 为OD数据添加车型分类
        toll_square_origins_with_class = toll_square_origins.copy()
        toll_square_origins_with_class['vehicle_class'] = toll_square_origins_with_class['vehicle_type'].apply(classify_vehicle_type)

        # 按收费广场编号和小时聚合OD数据，包含车型分析
        od_hourly = toll_square_origins_with_class.groupby(['start_square_code', 'start_hour']).agg({
            'pass_id': 'count',
            'vehicle_type': lambda x: x.value_counts().to_dict(),
            'vehicle_class': lambda x: x.value_counts().to_dict()
        }).reset_index()
        od_hourly.columns = ['square_code', 'hour', 'od_count', 'od_vehicle_types', 'od_vehicle_classes']

        # 计算车型占比
        def calculate_vehicle_ratios(vehicle_classes):
            total = sum(vehicle_classes.values())
            if total == 0:
                return {'passenger_ratio': 0, 'truck_ratio': 0, 'trailer_ratio': 0}
            return {
                'passenger_ratio': vehicle_classes.get('passenger', 0) / total,
                'truck_ratio': vehicle_classes.get('truck', 0) / total,
                'trailer_ratio': vehicle_classes.get('trailer', 0) / total
            }

        od_hourly['od_vehicle_ratios'] = od_hourly['od_vehicle_classes'].apply(calculate_vehicle_ratios)

        # 准备收费广场流量数据用于关联
        if hasattr(self, 'onramp_flow_data') and not self.onramp_flow_data.empty:
            # 添加小时字段到收费广场流量数据
            onramp_flow = self.onramp_flow_data.copy()
            onramp_flow['hour'] = pd.to_datetime(onramp_flow['start_time']).dt.hour

            # 按收费广场和小时聚合流量数据（入口流量）
            flow_hourly = onramp_flow[onramp_flow['direction'] == '入口'].groupby(['square_code', 'hour']).agg({
                'total': 'sum',
                'total_k': 'sum',
                'total_h': 'sum',
                'total_t': 'sum'
            }).reset_index()
            flow_hourly.columns = ['square_code', 'hour', 'flow_total', 'flow_k', 'flow_h', 'flow_t']

            # 计算流量数据的车型占比
            flow_hourly['flow_passenger_ratio'] = flow_hourly['flow_k'] / flow_hourly['flow_total']
            flow_hourly['flow_truck_ratio'] = flow_hourly['flow_h'] / flow_hourly['flow_total']
            flow_hourly['flow_trailer_ratio'] = flow_hourly['flow_t'] / flow_hourly['flow_total']
            flow_hourly = flow_hourly.fillna(0)

            # 关联OD数据和流量数据
            correlation_data = pd.merge(
                od_hourly,
                flow_hourly,
                on=['square_code', 'hour'],
                how='inner'
            )

            if not correlation_data.empty:
                # 计算相关性
                correlation_stats = {
                    'matched_records': len(correlation_data),
                    'total_od_records': len(od_hourly),
                    'match_ratio': len(correlation_data) / len(od_hourly) if len(od_hourly) > 0 else 0,
                    'correlation_coefficient': correlation_data['od_count'].corr(correlation_data['flow_total']) if len(correlation_data) > 1 else None
                }

                # 流量对比分析
                correlation_data['od_flow_ratio'] = correlation_data['od_count'] / correlation_data['flow_total']
                correlation_data['od_flow_ratio'] = correlation_data['od_flow_ratio'].fillna(0)

                # 提取车型占比数据
                od_ratios_df = pd.json_normalize(correlation_data['od_vehicle_ratios'])
                correlation_data = pd.concat([correlation_data, od_ratios_df], axis=1)

                # 车型占比对比分析
                correlation_data['truck_ratio_diff'] = correlation_data['truck_ratio'] - correlation_data['flow_truck_ratio']
                correlation_data['passenger_ratio_diff'] = correlation_data['passenger_ratio'] - correlation_data['flow_passenger_ratio']

                # 统计信息
                correlation_stats.update({
                    'avg_od_flow_ratio': correlation_data['od_flow_ratio'].mean(),
                    'median_od_flow_ratio': correlation_data['od_flow_ratio'].median(),
                    'max_od_flow_ratio': correlation_data['od_flow_ratio'].max(),
                    'min_od_flow_ratio': correlation_data['od_flow_ratio'].min(),
                    # 车型占比统计
                    'avg_od_truck_ratio': correlation_data['truck_ratio'].mean(),
                    'avg_flow_truck_ratio': correlation_data['flow_truck_ratio'].mean(),
                    'avg_truck_ratio_diff': correlation_data['truck_ratio_diff'].mean(),
                    'avg_od_passenger_ratio': correlation_data['passenger_ratio'].mean(),
                    'avg_flow_passenger_ratio': correlation_data['flow_passenger_ratio'].mean(),
                    'avg_passenger_ratio_diff': correlation_data['passenger_ratio_diff'].mean()
                })

                # 保存详细数据样例
                sample_columns = ['square_code', 'hour', 'od_count', 'flow_total', 'od_flow_ratio',
                                'truck_ratio', 'flow_truck_ratio', 'truck_ratio_diff',
                                'passenger_ratio', 'flow_passenger_ratio', 'passenger_ratio_diff']
                correlation_stats['sample_data'] = correlation_data[sample_columns].head(10).to_dict('records')

                return correlation_stats

        return {'error': '无法进行收费广场关联分析，缺少流量数据'}
    
    def analyze_od_flow_consistency(self) -> Dict:
        """分析OD流量与门架/收费广场流量的一致性"""
        logger.info("开始OD流量一致性分析")
        
        if self.gantry_flow_data is None or self.toll_flow_data is None:
            logger.warning("门架或收费广场流量数据未加载，跳过一致性分析")
            return {}
        
        consistency_results = {}
        
        # 按周统计OD流量
        od_weekly = self.od_data.groupby([
            pd.to_datetime(self.od_data['start_time']).dt.to_period('W'),
            'start_square_code'
        ]).size().reset_index(name='od_count')
        
        # 收费广场上匝道流量（对应OD起点）
        onramp_weekly = self.toll_flow_data[
            self.toll_flow_data['flow_type'] == 'onramp'
        ].groupby([
            pd.to_datetime(self.toll_flow_data['start_time']).dt.to_period('W'),
            'square_code'
        ])['total'].sum().reset_index()
        
        # 合并数据进行对比
        # 这里需要根据实际的数据关联关系进行调整
        
        consistency_results['od_weekly_count'] = len(od_weekly)
        consistency_results['onramp_weekly_count'] = len(onramp_weekly)
        
        logger.info("OD流量一致性分析完成")
        return consistency_results

    def create_visualizations(self, output_dir: str = "od_analysis_charts"):
        """创建数据可视化图表"""
        import os
        os.makedirs(output_dir, exist_ok=True)

        logger.info("开始创建可视化图表")

        # 1. OD对流量分布图
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))

        # 车辆类型分布
        vehicle_counts = self.od_data['vehicle_type'].value_counts()
        axes[0, 0].pie(vehicle_counts.values, labels=vehicle_counts.index, autopct='%1.1f%%')
        axes[0, 0].set_title('车辆类型分布')

        # 小时分布
        hourly_counts = self.od_data['hour'].value_counts().sort_index()
        axes[0, 1].bar(hourly_counts.index, hourly_counts.values)
        axes[0, 1].set_title('小时分布')
        axes[0, 1].set_xlabel('小时')
        axes[0, 1].set_ylabel('记录数')

        # 星期分布
        weekday_counts = self.od_data['weekday'].value_counts().sort_index()
        weekday_labels = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        axes[1, 0].bar(range(len(weekday_counts)), weekday_counts.values)
        axes[1, 0].set_title('星期分布')
        axes[1, 0].set_xlabel('星期')
        axes[1, 0].set_ylabel('记录数')
        axes[1, 0].set_xticks(range(len(weekday_labels)))
        axes[1, 0].set_xticklabels(weekday_labels)

        # 通行时间分布
        travel_time_filtered = self.od_data[
            (self.od_data['travel_time_hours'] > 0) &
            (self.od_data['travel_time_hours'] < 12)
        ]['travel_time_hours']
        axes[1, 1].hist(travel_time_filtered, bins=50, alpha=0.7)
        axes[1, 1].set_title('通行时间分布（0-12小时）')
        axes[1, 1].set_xlabel('通行时间（小时）')
        axes[1, 1].set_ylabel('频次')

        plt.tight_layout()
        plt.savefig(f'{output_dir}/od_basic_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        # 2. TOP OD对流量图
        top_od_pairs = self.od_data['od_pair'].value_counts().head(20)

        plt.figure(figsize=(12, 8))
        plt.barh(range(len(top_od_pairs)), top_od_pairs.values)
        plt.yticks(range(len(top_od_pairs)), top_od_pairs.index)
        plt.xlabel('流量（车次）')
        plt.title('TOP 20 OD对流量')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_od_pairs.png', dpi=300, bbox_inches='tight')
        plt.close()

        # 3. 收费站流量分布
        start_station_counts = self.od_data['start_station_name'].value_counts().head(20)
        end_station_counts = self.od_data['end_station_name'].value_counts().head(20)

        fig, axes = plt.subplots(1, 2, figsize=(20, 8))

        axes[0].barh(range(len(start_station_counts)), start_station_counts.values)
        axes[0].set_yticks(range(len(start_station_counts)))
        axes[0].set_yticklabels(start_station_counts.index)
        axes[0].set_xlabel('流量（车次）')
        axes[0].set_title('TOP 20 起点收费站流量')
        axes[0].invert_yaxis()

        axes[1].barh(range(len(end_station_counts)), end_station_counts.values)
        axes[1].set_yticks(range(len(end_station_counts)))
        axes[1].set_yticklabels(end_station_counts.index)
        axes[1].set_xlabel('流量（车次）')
        axes[1].set_title('TOP 20 终点收费站流量')
        axes[1].invert_yaxis()

        plt.tight_layout()
        plt.savefig(f'{output_dir}/station_flow_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"可视化图表已保存到 {output_dir} 目录")

    def generate_report(self, stats: Dict, anomalies: Dict, consistency: Dict,
                       correlation_analysis: Dict, output_file: str = "od_data_analysis_report.html"):
        """生成数据分析报告"""
        logger.info("开始生成数据分析报告")

        html_content = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>OD数据分析报告</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #e8f4fd; border-radius: 3px; }}
                .anomaly {{ background-color: #ffe6e6; }}
                .good {{ background-color: #e6ffe6; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>OD数据分析报告</h1>
                <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>分析期间: {stats.get('date_range', {}).get('start', 'N/A')} 至 {stats.get('date_range', {}).get('end', 'N/A')}</p>
            </div>

            <div class="section">
                <h2>1. 数据概览</h2>
                <div class="metric">总记录数: <strong>{stats.get('total_records', 0):,}</strong></div>
                <div class="metric">唯一通行ID: <strong>{stats.get('unique_pass_ids', 0):,}</strong></div>
                <div class="metric">唯一OD对: <strong>{stats.get('unique_od_pairs', 0):,}</strong></div>
                <div class="metric">起点收费站: <strong>{stats.get('unique_start_stations', 0)}</strong></div>
                <div class="metric">终点收费站: <strong>{stats.get('unique_end_stations', 0)}</strong></div>
                <div class="metric">起点收费广场: <strong>{stats.get('unique_start_squares', 0)}</strong></div>
                <div class="metric">终点收费广场: <strong>{stats.get('unique_end_squares', 0)}</strong></div>
            </div>

            <div class="section">
                <h2>2. 车辆类型分布</h2>
                <table>
                    <tr><th>车辆类型</th><th>数量</th><th>占比</th></tr>
        """

        # 添加车辆类型分布表格
        total_vehicles = sum(stats.get('vehicle_type_dist', {}).values())
        for vtype, count in stats.get('vehicle_type_dist', {}).items():
            percentage = (count / total_vehicles * 100) if total_vehicles > 0 else 0
            html_content += f"<tr><td>{vtype}</td><td>{count:,}</td><td>{percentage:.1f}%</td></tr>"

        html_content += """
                </table>

                <h3>车型分类统计（有效OD数据）</h3>
                <table>
                    <tr><th>车型分类</th><th>数量</th><th>占比</th></tr>
        """

        # 添加车型分类统计表格
        vehicle_class_dist = stats.get('vehicle_class_distribution', {})
        html_content += f"""
                    <tr><td>客车(K类)</td><td>{vehicle_class_dist.get('passenger_count', 0):,}</td><td>{vehicle_class_dist.get('passenger_ratio', 0):.1%}</td></tr>
                    <tr><td>货车(H类)</td><td>{vehicle_class_dist.get('truck_count', 0):,}</td><td>{vehicle_class_dist.get('truck_ratio', 0):.1%}</td></tr>
                    <tr><td>挂车(T类)</td><td>{vehicle_class_dist.get('trailer_count', 0):,}</td><td>{vehicle_class_dist.get('trailer_ratio', 0):.1%}</td></tr>
                    <tr><td>其他</td><td>{vehicle_class_dist.get('other_count', 0):,}</td><td>{vehicle_class_dist.get('other_ratio', 0):.1%}</td></tr>
        """

        html_content += """
                </table>
            </div>

            <div class="section">
                <h2>3. 数据异常检测</h2>
        """

        # 添加异常检测结果 - 只显示真正的异常
        anomaly_display_map = {
            'missing_start_codes': '起点编码完全缺失',
            'missing_end_codes': '终点编码完全缺失',
            'duplicate_records': '重复记录',
            'time_anomalies': '时间异常记录',
            'long_travel_time': '异常长通行时间',
            'short_travel_time': '异常短通行时间'
        }

        # 特殊情况（不算异常）
        special_cases_map = {
            'pass_through_vehicles': '穿过车辆（同起终点）'
        }

        for anomaly_type, display_name in anomaly_display_map.items():
            count = anomalies.get(anomaly_type, 0)
            if isinstance(count, (int, str)):
                count = int(count) if str(count).isdigit() else 0
                anomaly_class = "anomaly" if count > 0 else "good"
                html_content += f'<div class="metric {anomaly_class}">{display_name}: <strong>{count:,}</strong></div>'

        # 显示特殊情况
        for special_type, display_name in special_cases_map.items():
            count = anomalies.get(special_type, 0)
            if isinstance(count, (int, str)):
                count = int(count) if str(count).isdigit() else 0
                # 特殊情况用不同的样式
                html_content += f'<div class="metric" style="background-color: #fff3cd; border-color: #ffeaa7;">{display_name}: <strong>{count:,}</strong> <span style="color: #856404;">(特殊情况，非异常)</span></div>'

        # 显示关键字段空值（如果有）
        if anomalies.get('critical_null_values'):
            html_content += "<h3>关键字段空值</h3><ul>"
            for field, null_count in anomalies['critical_null_values'].items():
                html_content += f"<li>{field}: {null_count:,} 个空值</li>"
            html_content += "</ul>"

        # 添加异常样例展示
        anomaly_samples = anomalies.get('anomaly_samples', {})
        if anomaly_samples:
            html_content += """
            <div class="section">
                <h2>异常数据样例</h2>
                <p>以下是检测到的异常数据样例，便于分析和处理：</p>
            """

            # 异常样例展示映射
            anomaly_sample_display_map = {
                'time_anomalies': '时间异常样例',
                'long_travel_time': '异常长通行时间样例',
                'short_travel_time': '异常短通行时间样例',
                'duplicate_records': '重复记录样例',
                'missing_start_codes': '起点编码缺失样例',
                'missing_end_codes': '终点编码缺失样例'
            }

            # 特殊情况样例展示映射
            special_sample_display_map = {
                'pass_through_vehicles': '穿过车辆样例（同起终点，特殊情况）'
            }

            # 显示异常样例
            for sample_type, display_name in anomaly_sample_display_map.items():
                if sample_type in anomaly_samples:
                    samples = anomaly_samples[sample_type]
                    html_content += f"<h3>{display_name}</h3>"
                    html_content += "<table style='font-size: 12px;'>"

                    # 表头
                    if samples:
                        headers = list(samples[0].keys())
                        html_content += "<tr>"
                        for header in headers:
                            html_content += f"<th>{header}</th>"
                        html_content += "</tr>"

                        # 数据行
                        for sample in samples[:5]:  # 最多显示5条样例
                            html_content += "<tr>"
                            for header in headers:
                                value = sample.get(header, '')
                                # 格式化显示
                                if isinstance(value, float) and 'time' in header:
                                    value = f"{value:.4f}"
                                html_content += f"<td>{value}</td>"
                            html_content += "</tr>"

                    html_content += "</table><br>"

            # 显示特殊情况样例
            for sample_type, display_name in special_sample_display_map.items():
                if sample_type in anomaly_samples:
                    samples = anomaly_samples[sample_type]
                    html_content += f"<h3>{display_name}</h3>"
                    html_content += "<table style='font-size: 12px;'>"

                    # 表头
                    if samples:
                        headers = list(samples[0].keys())
                        html_content += "<tr>"
                        for header in headers:
                            html_content += f"<th>{header}</th>"
                        html_content += "</tr>"

                        # 数据行
                        for sample in samples[:5]:  # 最多显示5条样例
                            html_content += "<tr>"
                            for header in headers:
                                value = sample.get(header, '')
                                html_content += f"<td>{value}</td>"
                            html_content += "</tr>"

                    html_content += "</table><br>"

            html_content += "</div>"

        html_content += """
            </div>

            <div class="section">
                <h2>4. TOP 10 OD对</h2>
                <table>
                    <tr><th>排名</th><th>OD对</th><th>流量</th></tr>
        """

        # 添加TOP OD对表格
        for i, (od_pair, count) in enumerate(list(stats.get('top_od_pairs', {}).items())[:10], 1):
            html_content += f"<tr><td>{i}</td><td>{od_pair}</td><td>{count:,}</td></tr>"

        html_content += """
                </table>
            </div>

            <div class="section">
                <h2>5. 数据质量评估</h2>
                <p>基于以上分析，数据质量评估如下：</p>
                <ul>
        """

        # 数据质量评估
        quality_issues = []
        special_notes = []

        # 检查真正的异常情况
        if anomalies.get('critical_null_values'):
            quality_issues.append("存在关键字段空值，需要数据清洗")
        if anomalies.get('missing_start_codes', 0) > 0:
            quality_issues.append(f"存在 {anomalies['missing_start_codes']} 条起点编码完全缺失记录")
        if anomalies.get('missing_end_codes', 0) > 0:
            quality_issues.append(f"存在 {anomalies['missing_end_codes']} 条终点编码完全缺失记录")
        if anomalies.get('duplicate_records', 0) > 0:
            quality_issues.append(f"存在 {anomalies['duplicate_records']} 条重复记录")
        if anomalies.get('time_anomalies', 0) > 0:
            quality_issues.append(f"存在 {anomalies['time_anomalies']} 条时间异常记录")
        if anomalies.get('long_travel_time', 0) > 0:
            quality_issues.append(f"存在 {anomalies['long_travel_time']} 条异常长通行时间记录")
        if anomalies.get('short_travel_time', 0) > 0:
            quality_issues.append(f"存在 {anomalies['short_travel_time']} 条异常短通行时间记录")

        # 特殊情况说明
        if anomalies.get('pass_through_vehicles', 0) > 0:
            special_notes.append(f"检测到 {anomalies['pass_through_vehicles']} 条穿过车辆记录（同起终点），这些是特殊情况，不参与OD分析")

        if quality_issues:
            for issue in quality_issues:
                html_content += f"<li style='color: red;'>{issue}</li>"
        else:
            html_content += "<li style='color: green;'>数据质量良好，无明显异常</li>"

        # 添加特殊情况说明
        if special_notes:
            for note in special_notes:
                html_content += f"<li style='color: orange;'>{note}</li>"

        html_content += """
                </ul>
            </div>

            <div class="section">
                <h2>6. 数据结构说明</h2>
                <p>OD数据中的空值情况说明：</p>
                <ul>
        """

        # 添加数据结构说明
        data_info = anomalies.get('data_info', {})
        start_null_count = data_info.get('start_square_null_count', 0)
        end_null_count = data_info.get('end_square_null_count', 0)
        start_null_ratio = data_info.get('start_square_null_ratio', 0)
        end_null_ratio = data_info.get('end_square_null_ratio', 0)

        html_content += f"""
                    <li>起点square_code空值: {start_null_count:,} 条 ({start_null_ratio:.1%}) -
                        <span style="color: green;">正常情况，这些是门架起点</span></li>
                    <li>终点square_code空值: {end_null_count:,} 条 ({end_null_ratio:.1%}) -
                        <span style="color: green;">正常情况，这些是门架终点</span></li>
                    <li>OD编码规则: 优先使用square_code，为空时使用station_code（门架编号）</li>
                </ul>
            </div>

            <div class="section">
                <h2>7. 起始点关联分析</h2>
                <p>分析OD数据起始点与对应流量数据的关联性（排除同起终点的穿过车辆）：</p>
        """

        # 显示关联分析摘要
        if correlation_analysis and 'summary' in correlation_analysis:
            summary = correlation_analysis['summary']
            html_content += f"""
                <h3>关联分析摘要</h3>
                <ul>
                    <li>有效OD记录总数: {summary.get('total_valid_od', 0):,} 条</li>
                    <li>门架起点OD: {summary.get('gantry_origin_count', 0):,} 条 ({summary.get('gantry_origin_ratio', 0):.1%})</li>
                    <li>收费广场起点OD: {summary.get('toll_square_origin_count', 0):,} 条 ({summary.get('toll_square_origin_ratio', 0):.1%})</li>
                </ul>
            """

        # 门架关联分析结果
        if correlation_analysis and 'gantry_correlation' in correlation_analysis:
            gantry_corr = correlation_analysis['gantry_correlation']
            if 'error' not in gantry_corr:
                # 格式化相关系数
                corr_coeff = gantry_corr.get('correlation_coefficient')
                corr_str = f"{corr_coeff:.3f}" if corr_coeff is not None else 'N/A'

                html_content += f"""
                <h3>门架起点关联分析</h3>
                <ul>
                    <li>匹配记录数: {gantry_corr.get('matched_records', 0):,}</li>
                    <li>匹配率: {gantry_corr.get('match_ratio', 0):.1%}</li>
                    <li>相关系数: {corr_str}</li>
                    <li>平均OD/流量比: {gantry_corr.get('avg_od_flow_ratio', 0):.3f}</li>
                    <li>中位数OD/流量比: {gantry_corr.get('median_od_flow_ratio', 0):.3f}</li>
                </ul>
                <h4>车型占比对比</h4>
                <ul>
                    <li>OD数据货车占比: {gantry_corr.get('avg_od_truck_ratio', 0):.1%}</li>
                    <li>流量数据货车占比: {gantry_corr.get('avg_flow_truck_ratio', 0):.1%}</li>
                    <li>货车占比差异: {gantry_corr.get('avg_truck_ratio_diff', 0):+.1%}</li>
                    <li>OD数据客车占比: {gantry_corr.get('avg_od_passenger_ratio', 0):.1%}</li>
                    <li>流量数据客车占比: {gantry_corr.get('avg_flow_passenger_ratio', 0):.1%}</li>
                    <li>客车占比差异: {gantry_corr.get('avg_passenger_ratio_diff', 0):+.1%}</li>
                </ul>
                """
            else:
                html_content += f"<p>门架关联分析: {gantry_corr['error']}</p>"

        # 收费广场关联分析结果
        if correlation_analysis and 'toll_square_correlation' in correlation_analysis:
            toll_corr = correlation_analysis['toll_square_correlation']
            if 'error' not in toll_corr:
                # 格式化相关系数
                corr_coeff = toll_corr.get('correlation_coefficient')
                corr_str = f"{corr_coeff:.3f}" if corr_coeff is not None else 'N/A'

                html_content += f"""
                <h3>收费广场起点关联分析</h3>
                <ul>
                    <li>匹配记录数: {toll_corr.get('matched_records', 0):,}</li>
                    <li>匹配率: {toll_corr.get('match_ratio', 0):.1%}</li>
                    <li>相关系数: {corr_str}</li>
                    <li>平均OD/流量比: {toll_corr.get('avg_od_flow_ratio', 0):.3f}</li>
                    <li>中位数OD/流量比: {toll_corr.get('median_od_flow_ratio', 0):.3f}</li>
                </ul>
                <h4>车型占比对比</h4>
                <ul>
                    <li>OD数据货车占比: {toll_corr.get('avg_od_truck_ratio', 0):.1%}</li>
                    <li>流量数据货车占比: {toll_corr.get('avg_flow_truck_ratio', 0):.1%}</li>
                    <li>货车占比差异: {toll_corr.get('avg_truck_ratio_diff', 0):+.1%}</li>
                    <li>OD数据客车占比: {toll_corr.get('avg_od_passenger_ratio', 0):.1%}</li>
                    <li>流量数据客车占比: {toll_corr.get('avg_flow_passenger_ratio', 0):.1%}</li>
                    <li>客车占比差异: {toll_corr.get('avg_passenger_ratio_diff', 0):+.1%}</li>
                </ul>
                """
            else:
                html_content += f"<p>收费广场关联分析: {toll_corr['error']}</p>"

        html_content += """
            </div>

            <div class="section">
                <h2>8. 建议</h2>
                <ul>
                    <li>定期监控数据质量，及时发现和处理异常数据</li>
                    <li>建立数据清洗流程，处理空值和重复记录</li>
                    <li>对高流量OD对进行重点监控和分析</li>
                    <li>建立数据一致性检查机制</li>
                </ul>
            </div>
        </body>
        </html>
        """

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"数据分析报告已生成: {output_file}")

    def export_anomaly_data(self, anomalies: Dict, output_dir: str = "od_analysis_output"):
        """导出异常数据到Excel文件"""
        import os
        os.makedirs(output_dir, exist_ok=True)

        anomaly_samples = anomalies.get('anomaly_samples', {})
        if not anomaly_samples:
            logger.info("没有异常数据需要导出")
            return

        logger.info("开始导出异常数据")

        # 创建Excel写入器
        excel_file = os.path.join(output_dir, "anomaly_data_samples.xlsx")

        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # 导出各类异常数据
                sheet_names = {
                    'time_anomalies': '时间异常',
                    'pass_through_vehicles': '穿过车辆',
                    'long_travel_time': '异常长通行时间',
                    'short_travel_time': '异常短通行时间',
                    'duplicate_records': '重复记录',
                    'missing_start_codes': '起点编码缺失',
                    'missing_end_codes': '终点编码缺失'
                }

                for sample_type, sheet_name in sheet_names.items():
                    if sample_type in anomaly_samples:
                        df = pd.DataFrame(anomaly_samples[sample_type])
                        if not df.empty:
                            # 限制工作表名称长度
                            safe_sheet_name = sheet_name[:31]
                            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                            logger.info(f"导出 {sheet_name}: {len(df)} 条记录")

            logger.info(f"异常数据已导出到: {excel_file}")

        except Exception as e:
            logger.warning(f"导出异常数据失败: {e}")
            # 如果Excel导出失败，尝试导出为CSV
            try:
                for sample_type, samples in anomaly_samples.items():
                    if samples:
                        df = pd.DataFrame(samples)
                        csv_file = os.path.join(output_dir, f"anomaly_{sample_type}.csv")
                        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                        logger.info(f"导出CSV: {csv_file}")
            except Exception as csv_error:
                logger.error(f"CSV导出也失败: {csv_error}")

    def run_full_analysis(self, start_date: str, end_date: str,
                         limit: Optional[int] = None,
                         output_dir: str = "od_analysis_output"):
        """运行完整的数据分析流程"""
        import os
        os.makedirs(output_dir, exist_ok=True)

        logger.info("开始完整的OD数据分析流程")

        try:
            # 1. 连接数据库
            self.connect_database()

            # 2. 加载数据
            self.load_od_data(start_date, end_date, limit)
            self.load_flow_data(start_date, end_date)

            # 3. 基本统计分析
            stats = self.analyze_od_basic_stats()

            # 4. 异常检测
            anomalies = self.detect_od_anomalies()

            # 5. 起始点关联分析
            correlation_analysis = self.analyze_origin_correlation()

            # 6. 一致性分析
            consistency = self.analyze_od_flow_consistency()

            # 7. 创建可视化
            chart_dir = os.path.join(output_dir, "charts")
            self.create_visualizations(chart_dir)

            # 8. 生成报告
            report_file = os.path.join(output_dir, "od_analysis_report.html")
            self.generate_report(stats, anomalies, consistency, correlation_analysis, report_file)

            # 8. 导出异常数据
            self.export_anomaly_data(anomalies, output_dir)

            # 9. 保存详细数据
            stats_file = os.path.join(output_dir, "detailed_stats.json")
            import json
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'stats': stats,
                    'anomalies': anomalies,
                    'consistency': consistency,
                    'correlation_analysis': correlation_analysis
                }, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"完整分析流程完成，结果保存在: {output_dir}")

            return {
                'stats': stats,
                'anomalies': anomalies,
                'consistency': consistency,
                'output_dir': output_dir
            }

        except Exception as e:
            logger.error(f"分析流程执行失败: {e}")
            raise


def main():
    """主函数 - 示例用法"""

    # 导入配置
    try:
        # 添加config目录到路径
        config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
        sys.path.append(config_dir)
        from config import get_config, validate_config

        # 验证配置
        if not validate_config():
            print("❌ 配置验证失败，请设置环境变量或修改config.py")
            return

        # 获取配置
        config = get_config()
        db_config = config['database']

    except ImportError:
        print("❌ 无法导入配置文件，请确保config目录中有config.py文件")
        print("或者设置以下环境变量：")
        print("  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        return
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return

    # 创建分析器
    analyzer = ODDataAnalyzer(db_config)

    # 运行分析（示例：分析最近一周的数据）
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    try:
        results = analyzer.run_full_analysis(
            start_date=start_date,
            end_date=end_date,
            limit=10000,  # 测试时限制记录数
            output_dir="od_analysis_results"
        )

        print("分析完成！")
        print(f"总记录数: {results['stats']['total_records']:,}")
        print(f"唯一OD对: {results['stats']['unique_od_pairs']:,}")
        print(f"结果保存在: {results['output_dir']}")

    except Exception as e:
        print(f"分析失败: {e}")


if __name__ == "__main__":
    main()
