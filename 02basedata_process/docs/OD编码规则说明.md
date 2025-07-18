# OD编码规则说明

## 概述

在高速公路OD（Origin-Destination）数据分析中，需要正确处理起点和终点的编码规则。由于数据中存在收费广场和门架两种不同类型的点位，需要建立统一的编码规则。

## 数据字段说明

### OD表中的关键字段

| 字段名 | 类型 | 说明 | 可空性 |
|--------|------|------|--------|
| `start_square_code` | varchar(50) | 起始收费广场代码 | 可为空 |
| `start_station_code` | varchar(50) | 起始收费站代码/门架编号 | 不为空 |
| `end_square_code` | varchar(50) | 结束收费广场代码 | 可为空 |
| `end_station_code` | varchar(50) | 结束收费站代码/门架编号 | 不为空 |

### 字段含义解释

- **当 `square_code` 不为空时**：表示收费广场点位，`station_code` 为对应的收费站代码
- **当 `square_code` 为空时**：表示门架点位，`station_code` 为门架编号

## OD编码规则

### 1. 点位编码优先级

```
点位编码 = square_code (如果不为空) || station_code (如果square_code为空)
```

### 2. OD对构建规则

```
OD对 = 起点编码 + "-" + 终点编码
```

### 3. 实现逻辑（Python）

```python
# 构建起点和终点编码
data['start_point_code'] = data['start_square_code'].fillna(data['start_station_code'])
data['end_point_code'] = data['end_square_code'].fillna(data['end_station_code'])

# 构建OD对
data['od_pair'] = data['start_point_code'].astype(str) + '-' + data['end_point_code'].astype(str)
```

## OD对类型分类

### 1. 点位类型标记

```python
# 标记起点类型
data['start_point_type'] = data['start_square_code'].apply(
    lambda x: 'toll_square' if pd.notna(x) else 'gantry'
)

# 标记终点类型
data['end_point_type'] = data['end_square_code'].apply(
    lambda x: 'toll_square' if pd.notna(x) else 'gantry'
)

# 构建OD对类型
data['od_pair_type'] = data['start_point_type'] + '_to_' + data['end_point_type']
```

### 2. OD对类型说明

| OD对类型 | 说明 | 示例 |
|----------|------|------|
| `toll_square_to_toll_square` | 收费广场到收费广场 | SQ001-SQ002 |
| `toll_square_to_gantry` | 收费广场到门架 | SQ001-GT001 |
| `gantry_to_toll_square` | 门架到收费广场 | GT001-SQ001 |
| `gantry_to_gantry` | 门架到门架 | GT001-GT002 |

## 测试用例

### 测试数据示例

| pass_id | start_square_code | start_station_code | end_square_code | end_station_code | 期望OD对 | OD对类型 |
|---------|-------------------|-------------------|-----------------|------------------|----------|----------|
| P001 | SQ001 | ST001 | SQ002 | ST002 | SQ001-SQ002 | toll_square_to_toll_square |
| P002 | NULL | GT001 | SQ002 | ST002 | GT001-SQ002 | gantry_to_toll_square |
| P003 | SQ003 | ST003 | NULL | GT003 | SQ003-GT003 | toll_square_to_gantry |
| P004 | NULL | GT002 | NULL | GT004 | GT002-GT004 | gantry_to_gantry |

### 验证结果

所有测试用例均通过验证 ✓

## 数据质量检查

### 1. 必要的异常检测

```python
# 检测既没有square_code也没有station_code的记录
missing_both_start = data[
    data['start_square_code'].isna() & data['start_station_code'].isna()
]

missing_both_end = data[
    data['end_square_code'].isna() & data['end_station_code'].isna()
]
```

### 2. 同起终点检测

```python
# 使用处理后的point_code检测同起终点
same_origin_dest = data[data['start_point_code'] == data['end_point_code']]
```

## 应用场景

### 1. OD重要性分级

- 不同类型的OD对可能有不同的重要性权重
- 收费广场间的OD对通常代表完整的高速公路行程
- 门架间的OD对可能代表路段内的通行

### 2. 流量分析

- 按OD对类型进行流量统计
- 分析不同类型OD对的流量特征
- 识别关键的OD走廊

### 3. 异常检测

- 检测编码缺失的异常数据
- 识别不合理的OD组合
- 验证数据完整性

## 注意事项

1. **编码一致性**：确保在整个分析流程中使用统一的编码规则
2. **空值处理**：必须正确处理 `square_code` 为空的情况
3. **类型区分**：在分析时需要考虑不同OD对类型的特殊性
4. **数据验证**：定期检查编码逻辑的正确性

## 更新记录

- 2025-01-17: 初始版本，定义OD编码规则和实现逻辑
- 2025-01-17: 添加测试用例和验证结果
- 2025-01-17: 更新流量数据关键表结构说明文档
