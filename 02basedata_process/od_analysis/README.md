# 高速公路OD与流量关联分析工具

## 📋 项目简介

本工具用于分析高速公路OD（Origin-Destination）数据与流量数据之间的关联性，重点解决门架途中流量和收费广场进出流量的准确对比分析问题。

## 🎯 核心功能

### 1. 门架分析
- **门架起点/终点分析**：统计门架作为起点和终点的OD数据与流量关联
- **门架途中流量分析**：计算门架的途中流量占比和功能分类
- **门架功能分类**：将门架分为通道型、混合型、起止型

### 2. 收费广场分析
- **收费广场入口/出口分析**：统计收费广场进出流量与OD数据的关联
- **流量平衡分析**：分析收费广场进出流量的平衡情况
- **一致性评估**：评估收费广场流量与OD数据的一致性

### 3. 深度分析
- **中位数案例分析**：深入分析OD/流量比中位数附近的具体案例
- **车型结构对比**：对比OD数据与流量数据的车型分布差异
- **异常模式识别**：识别数据质量异常和流量模式异常

## 🚀 快速开始

### 环境要求
- Python 3.7+
- PostgreSQL数据库访问权限
- 必要的Python包：pandas, numpy, sqlalchemy, psycopg2

### 安装依赖
```bash
pip install pandas numpy sqlalchemy psycopg2-binary
```

### 配置数据库连接
```bash
# Windows PowerShell
$env:DB_HOST="your_host"
$env:DB_PORT="5432"
$env:DB_NAME="your_database"
$env:DB_USER="your_username"
$env:DB_PASSWORD="your_password"
```

### 运行分析
```bash
# 基本用法（分析1小时数据）
python detailed_correlation_analysis.py --start-date "2025-07-07 08:00:00" --end-date "2025-07-07 09:00:00"

# 快速测试（分析30分钟数据）
python detailed_correlation_analysis.py --start-date "2025-07-07 08:00:00" --end-date "2025-07-07 08:30:00"
```

## 📊 主要发现

### 门架分析结果
- **途中流量占比**：93.7%（验证了门架主要承担通道功能）
- **门架功能分类**：88.5%为通道型，10.6%为混合型，0.9%为起止型

### 收费广场分析结果
- **相关系数**：0.953（验证了收费广场流量与OD数据高度一致）
- **中位数OD/流量比**：约50%（属于正常现象）

### 差异原因分析
- **OD数据采样不完整**：约50%的捕获率
- **流量统计更全面**：物理检测设备捕获率更高
- **车型分类一致性**：结构比例基本一致，差异在数量层面

## 📁 文件结构

```
od_analysis/
├── detailed_correlation_analysis.py    # 主分析脚本
├── run_detailed_correlation.py         # 运行脚本（简化版）
├── debug_data_structure.py            # 数据结构调试脚本
├── 高速公路OD与流量关联分析总结报告.md    # 完整分析报告
├── 快速使用指南.md                     # 快速使用指南
├── 高速公路OD与流量关联分析优化方案.md    # 优化方案文档
├── README.md                          # 本文件
└── detailed_correlation_output/        # 输出目录
    ├── detailed_correlation_report.html
    ├── gantry_origin_analysis.json
    ├── gantry_destination_analysis.json
    ├── gantry_transit_analysis.json
    ├── toll_square_entry_analysis.json
    ├── toll_square_exit_analysis.json
    ├── toll_square_balance_analysis.json
    ├── toll_square_median_entry_analysis.json
    └── toll_square_median_exit_analysis.json
```

## 📈 输出说明

### HTML报告
- **完整的可视化报告**：包含所有分析结果和图表
- **浏览器查看**：双击HTML文件在浏览器中打开

### JSON数据文件
- **详细的分析数据**：可用于进一步处理和分析
- **结构化格式**：便于程序化处理和集成

## 🔧 参数说明

| 参数 | 说明 | 示例 |
|------|------|------|
| `--start-date` | 开始时间 | "2025-07-07 08:00:00" |
| `--end-date` | 结束时间 | "2025-07-07 09:00:00" |
| `--sample-size` | 样本大小（可选） | 10000 |
| `--output-dir` | 输出目录 | "custom_output" |

## 📊 关键指标

### 正常范围参考
- **门架途中流量占比**：90%-95%
- **收费广场相关系数**：0.9-1.0
- **收费广场OD/流量比**：45%-55%
- **流量平衡比**：0.7-1.3

### 异常阈值
- **OD/流量比偏离50%超过20%**：需要调查
- **门架途中流量占比低于80%**：需要检查
- **收费广场相关系数低于0.8**：数据质量问题

## 🛠️ 故障排除

### 常见问题
1. **数据库连接失败**：检查环境变量和网络连接
2. **内存不足**：减少时间范围或使用样本限制
3. **关联数据为空**：检查时间范围设置
4. **字段不存在**：检查数据表结构

### 性能优化
- **推荐时间窗口**：1小时（平衡分析质量和性能）
- **大数据集处理**：使用`--sample-size`参数
- **并行处理**：可以同时分析多个时间段

## 📚 相关文档

- [高速公路OD与流量关联分析总结报告](./高速公路OD与流量关联分析总结报告.md) - 完整的分析结果和业务洞察
- [快速使用指南](./快速使用指南.md) - 快速上手指南
- [高速公路OD与流量关联分析优化方案](./高速公路OD与流量关联分析优化方案.md) - 技术优化方案

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个工具。

## 📄 许可证

本项目采用MIT许可证。

---

**版本**：v2.0  
**最后更新**：2025-07-18  
**维护者**：交通数据分析团队
