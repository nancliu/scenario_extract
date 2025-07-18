#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行详细起始点关联分析的脚本

使用示例:
python run_detailed_correlation.py --start-date 2025-07-07 --end-date 2025-07-14 --sample-size 100000
"""

import os
import sys
import subprocess

def main():
    """主函数"""
    # 设置环境变量
    os.environ['DB_HOST'] = '10.149.235.123'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'sdzg'
    os.environ['DB_USER'] = 'ln'
    os.environ['DB_PASSWORD'] = 'caneln'
    
    # 默认参数
    start_date = '2025-07-07'
    end_date = '2025-07-14'
    sample_size = 100000
    output_dir = 'detailed_correlation_output'
    
    # 解析命令行参数
    if len(sys.argv) > 1:
        for i, arg in enumerate(sys.argv):
            if arg == '--start-date' and i + 1 < len(sys.argv):
                start_date = sys.argv[i + 1]
            elif arg == '--end-date' and i + 1 < len(sys.argv):
                end_date = sys.argv[i + 1]
            elif arg == '--sample-size' and i + 1 < len(sys.argv):
                sample_size = int(sys.argv[i + 1])
            elif arg == '--output-dir' and i + 1 < len(sys.argv):
                output_dir = sys.argv[i + 1]
    
    print("=" * 80)
    print("详细起始点关联分析")
    print("=" * 80)
    print(f"参数设置:")
    print(f"  开始日期: {start_date}")
    print(f"  结束日期: {end_date}")
    print(f"  样本大小: {sample_size:,}")
    print(f"  输出目录: {output_dir}")
    print("=" * 80)
    
    # 构建命令
    cmd = [
        'python', 'detailed_correlation_analysis.py',
        '--start-date', start_date,
        '--end-date', end_date,
        '--sample-size', str(sample_size),
        '--output-dir', output_dir
    ]
    
    # 执行命令
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"执行失败: {e}")
        return e.returncode
    except Exception as e:
        print(f"运行错误: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
