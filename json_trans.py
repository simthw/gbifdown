#!/usr/bin/env python
# JSON to Excel 转换工具 - 用于 GBIF 数据

import os
import json
import pandas as pd

def get_gbif_standard_fields():
    """返回GBIF网站标准字段列表，按照网站显示顺序排列"""
    return [
        'Scientific name', 'Country or area', 'Coordinates', 'Event date',
        'Occurrence status', 'Basis of record', 'Dataset', 'Issues',
        'Type status', 'Preparations', 'Individual count', 'Organism quantity',
        'Organism quantity type', 'Sample size unit', 'Sample size value',
        'Record number', 'Recorded by', 'Catalogue number', 'Collection code',
        'Institution code', 'Occurrence ID', 'Identified by', 'Publisher',
        'Locality', 'Water body', 'State province', 'County', 'Municipality',
        'Continent', 'Island', 'Island group', 'Depth', 'Elevation',
        'Habitat', 'Field number', 'Identification ID', 'Other catalogue numbers',
        'Life stage', 'Sex', 'Establishment means', 'Degree of establishment',
        'Pathway', 'Behavior', 'Occurrence remarks', 'Identification remarks',
        'Identification qualifier', 'Higher geography', 'Higher classification',
        'Identification verification status', 'Geodetic datum', 'Coordinate uncertainty in meters',
        'Coordinate precision', 'Georeference protocol', 'Georeference remarks',
        'Georeference sources', 'Georeference verification status', 'Identified by ID',
        'Recorded by ID', 'Scientific name ID', 'Accepted scientific name',
        'Accepted scientific name ID', 'Vernacular name', 'License',
        'Rights', 'Rights holder', 'Owner institution code', 'Record history',
        'Higher geography ID', 'Protocol', 'Modified', 'Last interpreted',
        'Last crawled', 'Last parsed', 'Crawl ID', 'Recording number',
        'Media', 'GBIF taxon ID', 'Taxonomic status', 'Identification references',
        'Date identified', 'Rank', 'Kingdom', 'Phylum', 'Class', 'Order', 'Family',
        'Genus', 'Species', 'Infraspecific epithet', 'Generic name', 'Specific epithet',
        'Parent', 'Parent key', 'HTTP response'
    ]

def convert_json_to_excel():
    """将所有下载的 JSON 文件转换为 Excel 格式"""
    print("开始将 JSON 数据转换为 Excel...")
    os.makedirs('xlsx_data', exist_ok=True)
    
    # 获取所有 JSON 文件
    json_files = [f for f in os.listdir('occurrences') if f.endswith('.json')]
    print(f"找到 {len(json_files)} 个物种的 JSON 数据")
    
    # 获取标准字段列表
    standard_fields = get_gbif_standard_fields()
    
    total_processed = 0
    for json_file in json_files:
        species_name = json_file.replace('_', ' ').replace('.json', '')
        xlsx_filename = json_file.replace('.json', '.xlsx')
        json_path = os.path.join('occurrences', json_file)
        xlsx_path = os.path.join('xlsx_data', xlsx_filename)
        
        # 跳过已存在的 Excel 文件
        if os.path.exists(xlsx_path):
            print(f"跳过 {species_name} (Excel 已存在)")
            continue
            
        print(f"处理 {species_name}...")
        
        try:
            # 读取 JSON 文件
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not data:
                print(f"  {species_name} 没有数据记录")
                continue
                
            # 提取字段，与GBIF网站显示的格式一致
            records = []
            for record in data:
                # 处理日期格式
                event_date = record.get('eventDate', '')
                
                # 坐标格式化
                coordinates = ''
                if record.get('decimalLatitude') is not None and record.get('decimalLongitude') is not None:
                    coordinates = f"{record.get('decimalLatitude')}, {record.get('decimalLongitude')}"
                
                # 构建基础记录
                extracted = {
                    'Scientific name': record.get('scientificName', ''),
                    'Country or area': record.get('country', record.get('countryCode', '')),
                    'Coordinates': coordinates,
                    'Event date': event_date,
                    'Occurrence status': record.get('occurrenceStatus', ''),
                    'Basis of record': record.get('basisOfRecord', ''),
                    'Dataset': record.get('datasetName', ''),
                    'Issues': ','.join(record.get('issues', [])) if isinstance(record.get('issues'), list) else '',
                    'Type status': record.get('typeStatus', ''),
                    'Preparations': record.get('preparations', ''),
                    'Individual count': record.get('individualCount', ''),
                    'Organism quantity': record.get('organismQuantity', ''),
                    'Organism quantity type': record.get('organismQuantityType', ''),
                    'Sample size unit': record.get('sampleSizeUnit', ''),
                    'Sample size value': record.get('sampleSizeValue', ''),
                    'Record number': record.get('recordNumber', ''),
                    'Recorded by': record.get('recordedBy', ''),
                    'Catalogue number': record.get('catalogNumber', ''),
                    'Collection code': record.get('collectionCode', ''),
                    'Institution code': record.get('institutionCode', ''),
                    'Occurrence ID': record.get('occurrenceID', ''),
                    'Identified by': record.get('identifiedBy', ''),
                    'Publisher': record.get('publisher', record.get('publishingOrgKey', '')),
                    'Locality': record.get('locality', ''),
                    'Water body': record.get('waterBody', ''),
                    'State province': record.get('stateProvince', ''),
                    'County': record.get('county', ''),
                    'Municipality': record.get('municipality', ''),
                    'Continent': record.get('continent', ''),
                    'Island': record.get('island', ''),
                    'Island group': record.get('islandGroup', ''),
                    'Depth': record.get('depth', ''),
                    'Elevation': record.get('elevation', ''),
                    'Habitat': record.get('habitat', ''),
                    'Field number': record.get('fieldNumber', ''),
                    'Identification ID': record.get('identificationID', ''),
                    'Other catalogue numbers': record.get('otherCatalogNumbers', ''),
                    'Life stage': record.get('lifeStage', ''),
                    'Sex': record.get('sex', ''),
                    'Establishment means': record.get('establishmentMeans', ''),
                    'Degree of establishment': record.get('degreeOfEstablishment', ''),
                    'Pathway': record.get('pathway', ''),
                    'Behavior': record.get('behavior', ''),
                    'Occurrence remarks': record.get('occurrenceRemarks', ''),
                    'Identification remarks': record.get('identificationRemarks', ''),
                    'Identification qualifier': record.get('identificationQualifier', ''),
                    'Higher geography': record.get('higherGeography', ''),
                    'Higher classification': record.get('higherClassification', ''),
                    'Identification verification status': record.get('identificationVerificationStatus', ''),
                    'Geodetic datum': record.get('geodeticDatum', ''),
                    'Coordinate uncertainty in meters': record.get('coordinateUncertaintyInMeters', ''),
                    'Coordinate precision': record.get('coordinatePrecision', ''),
                    'License': record.get('license', ''),
                    'Rights holder': record.get('rightsHolder', ''),
                    'Modified': record.get('modified', ''),
                    'Last interpreted': record.get('lastInterpreted', ''),
                    'Last crawled': record.get('lastCrawled', ''),
                    'Last parsed': record.get('lastParsed', ''),
                    'Media': ','.join([m.get('identifier', '') for m in record.get('media', [])]) if isinstance(record.get('media'), list) else '',
                    'GBIF taxon ID': record.get('taxonKey', ''),
                    'Taxonomic status': record.get('taxonomicStatus', ''),
                    'Date identified': record.get('dateIdentified', ''),
                    'Rank': record.get('taxonRank', ''),
                    'Kingdom': record.get('kingdom', ''),
                    'Phylum': record.get('phylum', ''),
                    'Class': record.get('class', ''),
                    'Order': record.get('order', ''),
                    'Family': record.get('family', ''),
                    'Genus': record.get('genus', ''),
                    'Species': record.get('species', species_name)
                }
                
                # 添加其他可能的字段
                for key, value in record.items():
                    field_name = key.replace('_', ' ').title().replace('Id', 'ID')
                    if field_name not in extracted and field_name in standard_fields:
                        extracted[field_name] = value
                
                records.append(extracted)
                
            # 转换为 DataFrame，按照标准字段排序
            df = pd.DataFrame(records)
            
            # 只保留存在的标准字段，按顺序排列
            existing_fields = [field for field in standard_fields if field in df.columns]
            df = df[existing_fields]
            
            # 保存为Excel
            df.to_excel(xlsx_path, index=False, engine='openpyxl')
            print(f"  已保存 {len(records)} 条记录到 {xlsx_filename}")
            total_processed += 1
            
            # 每处理10个文件显示进度
            if total_processed % 10 == 0:
                print(f"已处理 {total_processed}/{len(json_files)} 个文件 ({round(total_processed/len(json_files)*100, 2)}%)")
            
        except Exception as e:
            print(f"  处理 {species_name} 时出错: {e}")
    
    print("Excel 转换完成！")

# 合并CSV功能已移除

def main():
    """主函数"""
    print("GBIF JSON 到 Excel 转换工具")
    print("========================")
    
    # 转换 JSON 到 Excel
    convert_json_to_excel()
    
    print("处理完成!")

if __name__ == "__main__":
    main()