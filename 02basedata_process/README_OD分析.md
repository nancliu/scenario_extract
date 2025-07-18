# OD数据分析脚本使用说明

## 概述

本脚本用于分析高速公路OD（Origin-Destination）事实表数据，为OD重要性分级提供数据基础。脚本实现了数据特征分析、异常检测、流量一致性分析等功能。

## 功能特性

### 1. 数据特征分析
- OD数据基本统计信息
- 车辆类型分布分析
- 时间分布分析（小时、星期）
- OD对数量和流量分布
- 收费站流量统计

### 2. 数据异常检测
- 空值检测
- 重复记录检测
- 时间异常检测（开始时间晚于结束时间）
- 同起终点检测
- 异常通行时间检测

### 3. 流量一致性分析
- OD流量与门架流量对比
- OD流量与收费广场流量对比
- 数据一致性评估

### 4. 可视化输出
- 数据分布图表
- TOP OD对流量图
- 收费站流量分布图
- 时间分布图

### 5. 报告生成
- HTML格式数据分析报告
- 详细统计数据JSON文件
- 数据质量评估

## 文件结构

```
02basedata_process/
├── od_data_analysis.py      # 主分析脚本
├── config.py                # 配置文件
├── README_OD分析.md         # 使用说明（本文件）
├── 流量数据关键表结构说明.md  # 数据表结构说明
└── OD重要性分级说明.md       # OD分级理论说明
```

## 环境要求

### Python版本
- Python 3.7+

### 依赖包
```bash
pip install pandas numpy matplotlib seaborn plotly psycopg2-binary sqlalchemy
```

### 数据库
- PostgreSQL 数据库
- 包含以下表：
  - `dwd.dwd_od_weekly` - OD事实表
  - `dwd.dwd_flow_gantry_weekly` - 门架流量表
  - `dwd.dwd_flow_onramp_weekly` - 上匝道流量表
  - `dwd.dwd_flow_offramp_weekly` - 下匝道流量表

## 配置说明

### 1. 数据库配置
编辑 `config.py` 文件中的 `DATABASE_CONFIG`：

```python
DATABASE_CONFIG = {
    'host': 'your_db_host',
    'port': '5432',
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}
```

### 2. 环境变量配置（推荐）
```bash
export DB_HOST=your_db_host
export DB_PORT=5432
export DB_NAME=your_database
export DB_USER=your_username
export DB_PASSWORD=your_password
```

## 使用方法

### 1. 基本使用
```python
from od_data_analysis import ODDataAnalyzer
from config import DATABASE_CONFIG

# 创建分析器
analyzer = ODDataAnalyzer(DATABASE_CONFIG)

# 运行完整分析
results = analyzer.run_full_analysis(
    start_date='2024-01-01',
    end_date='2024-01-08',
    output_dir='analysis_results'
)
```

### 2. 命令行使用
```bash
python od_data_analysis.py
```

### 3. 分步骤使用
```python
# 1. 连接数据库
analyzer.connect_database()

# 2. 加载数据
analyzer.load_od_data('2024-01-01', '2024-01-08')
analyzer.load_flow_data('2024-01-01', '2024-01-08')

# 3. 分析
stats = analyzer.analyze_od_basic_stats()
anomalies = analyzer.detect_od_anomalies()
consistency = analyzer.analyze_od_flow_consistency()

# 4. 生成报告
analyzer.create_visualizations('charts')
analyzer.generate_report(stats, anomalies, consistency, 'report.html')
```

## 输出说明

### 1. 目录结构
```
analysis_results/
├── charts/                          # 可视化图表
│   ├── od_basic_distribution.png    # 基本分布图
│   ├── top_od_pairs.png            # TOP OD对图
│   └── station_flow_distribution.png # 收费站流量分布图
├── od_analysis_report.html         # HTML分析报告
└── detailed_stats.json             # 详细统计数据
```

### 2. 报告内容
- **数据概览**: 总记录数、唯一OD对数量等
- **车辆类型分布**: 各车型占比统计
- **数据异常检测**: 异常数据统计和分析
- **TOP OD对**: 流量最高的OD对列表
- **数据质量评估**: 综合质量评分和建议

## 参数配置

### 1. 分析参数
在 `config.py` 中的 `ANALYSIS_CONFIG` 配置：

```python
ANALYSIS_CONFIG = {
    'max_travel_time_hours': 24,     # 最大合理通行时间
    'min_travel_time_minutes': 1,    # 最小合理通行时间
    'max_null_ratio': 0.05,          # 最大空值比例
    'top_n_od_pairs': 20,            # 显示TOP N个OD对
}
```

### 2. OD重要性分级参数
```python
OD_IMPORTANCE_CONFIG = {
    'high_flow_percentile': 90,      # 高流量阈值（前10%）
    'medium_flow_percentile': 50,    # 中流量阈值（10%-50%）
    'key_station_keywords': ['省界', '枢纽', '互通'],
}
```

## 常见问题

### 1. 数据库连接失败
- 检查数据库配置是否正确
- 确认数据库服务是否启动
- 检查网络连接和防火墙设置

### 2. 内存不足
- 使用 `limit` 参数限制数据量
- 分批处理大数据集
- 增加系统内存

### 3. 图表显示异常
- 确认已安装中文字体
- 检查matplotlib配置
- 更新相关依赖包

### 4. 数据表不存在
- 确认数据表名称正确
- 检查数据库权限
- 验证表结构是否符合要求

## 扩展功能

### 1. 自定义分析指标
可以继承 `ODDataAnalyzer` 类，添加自定义分析方法：

```python
class CustomODAnalyzer(ODDataAnalyzer):
    def custom_analysis(self):
        # 自定义分析逻辑
        pass
```

### 2. 数据导出
支持将分析结果导出为多种格式：

```python
# 导出为Excel
stats_df = pd.DataFrame(stats)
stats_df.to_excel('od_stats.xlsx')

# 导出为CSV
od_summary = analyzer.od_data.groupby('od_pair').size()
od_summary.to_csv('od_summary.csv')
```

## 性能优化建议

1. **数据分区**: 按时间分区查询，避免全表扫描
2. **索引优化**: 确保关键字段有适当索引
3. **批处理**: 大数据集分批处理
4. **缓存**: 缓存中间计算结果
5. **并行处理**: 使用多进程处理大数据集

## 联系方式

如有问题或建议，请联系：
- 邮箱: data-team@example.com
- 项目地址: /projects/scenario_extract

## 更新日志

- v1.0 (2025-01-17): 初始版本发布
  - 基本数据分析功能
  - 异常检测功能
  - 可视化和报告生成
