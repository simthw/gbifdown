#!/usr/bin/env python
# JSON to CSV 转换工具 - 用于 GBIF 数据

import os
import json
import pandas as pd

def convert_json_to_csv():
    """将所有下载的 JSON 文件转换为 CSV 格式"""
    print("开始将 JSON 数据转换为 CSV...")
    os.makedirs('csv_data', exist_ok=True)
    
    # 获取所有 JSON 文件
    json_files = [f for f in os.listdir('occurrences') if f.endswith('.json')]
    print(f"找到 {len(json_files)} 个物种的 JSON 数据")
    
    for json_file in json_files:
        species_name = json_file.replace('_', ' ').replace('.json', '')
        csv_filename = json_file.replace('.json', '.csv')
        json_path = os.path.join('occurrences', json_file)
        csv_path = os.path.join('csv_data', csv_filename)
        
        # 跳过已存在的 CSV 文件
        if os.path.exists(csv_path):
            print(f"跳过 {species_name} (CSV 已存在)")
            continue
            
        print(f"处理 {species_name}...")
        
        try:
            # 读取 JSON 文件
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not data:
                print(f"  {species_name} 没有数据记录")
                continue
                
            # 提取感兴趣的字段（可以根据需要调整）
            records = []
            for record in data:
                extracted = {
                    'species': species_name,
                    'gbifID': record.get('gbifID', ''),
                    'scientificName': record.get('scientificName', ''),
                    'countryCode': record.get('countryCode', ''),
                    'locality': record.get('locality', ''),
                    'decimalLatitude': record.get('decimalLatitude', ''),
                    'decimalLongitude': record.get('decimalLongitude', ''),
                    'elevation': record.get('elevation', ''),
                    'eventDate': record.get('eventDate', ''),
                    'year': record.get('year', ''),
                    'month': record.get('month', ''),
                    'day': record.get('day', ''),
                    'basisOfRecord': record.get('basisOfRecord', ''),
                    'recordedBy': record.get('recordedBy', '')
                }
                records.append(extracted)
                
            # 转换为 DataFrame 并保存为 CSV
            df = pd.DataFrame(records)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"  已保存 {len(records)} 条记录到 {csv_filename}")
            
        except Exception as e:
            print(f"  处理 {species_name} 时出错: {e}")
    
    print("CSV 转换完成！")

# 合并CSV功能已移除

def main():
    """主函数"""
    print("GBIF JSON 到 CSV 转换工具")
    print("========================")
    
    # 转换 JSON 到 CSV
    convert_json_to_csv()
    
    print("处理完成!")

if __name__ == "__main__":
    main()