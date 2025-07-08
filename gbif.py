# anaconda，python=3.12, env=gbif
# 高效下载GBIF物种分布数据，与网站格式一致

import os
import pandas as pd
import concurrent.futures
import time
import json
import requests
from datetime import datetime

# 全局参数
MAX_RECORDS_PER_SPECIES = 1e7  # 每个物种最多下载的记录数
REQUEST_DELAY = 0.3            # 请求之间的延迟秒数
PARALLEL_DOWNLOADS = 2         # 并行下载线程数
RETRY_COUNT = 3               # 出错时最大重试次数
RETRY_DELAY = 5               # 重试之间的等待秒数
DEBUG_MODE = True             # 是否输出详细信息

# 创建目录保存下载的数据
os.makedirs('occurrences', exist_ok=True)

def download_species(species_name, index=None, total=None):
    """下载单一物种的分布数据"""
    # 将物种名转换为文件名
    species_filename = species_name.replace(' ', '_') + '.json'
    species_path = os.path.join('occurrences', species_filename)
    
    # 如果已下载，跳过
    if os.path.exists(species_path):
        if index is not None and total is not None:
            print(f"跳过物种 {index}/{total}: {species_name} (已下载)")
        return 0
    
    if index is not None and total is not None:
        print(f"处理物种 {index}/{total}: {species_name}")
    
    try:
        # GBIF API URL，设置参数与网站格式一致
        base_url = "https://api.gbif.org/v1/occurrence/search"
        
        # 参数设置，与GBIF网站保持一致
        params = {
            'scientificName': species_name,
            'limit': 300,  # 每页结果数量
            'offset': 0,
            'status': 'ACCEPTED',          # 仅接受有效的分类单元名称
            'occurrenceStatus': 'PRESENT', # 仅包括存在记录
            'hasCoordinate': 'true',       # 仅包括有坐标的记录
            'hasGeospatialIssue': 'false', # 排除有地理空间问题的记录
            'advanced': 'true'             # 启用高级查询模式，与网站一致
        }
        
        all_results = []
        record_count = 0
        page_count = 0
        total_records = None
        
        # 使用分页获取所有结果
        while True:
            retry_count = 0
            while retry_count < RETRY_COUNT:
                try:
                    # 添加延迟避免API限制
                    if page_count > 0:
                        time.sleep(REQUEST_DELAY)
                    
                    response = requests.get(base_url, params=params)
                    response.raise_for_status()  # 如有错误则引发异常
                    
                    data = response.json()
                    page_count += 1
                    
                    # 第一次请求获取总记录数
                    if total_records is None:
                        total_records = data.get('count', 0)
                        if DEBUG_MODE:
                            print(f"  {species_name}: 总记录数 {total_records}")
                    
                    if 'results' not in data or not data['results']:
                        if DEBUG_MODE:
                            print(f"  {species_name}: 无结果，结束查询")
                        break
                    
                    results = data['results']
                    all_results.extend(results)
                    record_count += len(results)
                    
                    # 显示简洁进度信息
                    progress_interval = 5 if total_records > 10000 else 2
                    if page_count % progress_interval == 0 or len(results) < params['limit']:
                        percentage = round((record_count / total_records * 100), 2) if total_records > 0 else 0
                        print(f"  {species_name}: 已获取 {record_count}/{total_records} 条记录 ({percentage}%)")
                    
                    # 检查是否已获取所有结果或达到上限
                    if len(results) < params['limit'] or record_count >= MAX_RECORDS_PER_SPECIES or record_count >= total_records:
                        if DEBUG_MODE:
                            print(f"  {species_name}: 已达到结束条件，获取完毕")
                        break
                    
                    params['offset'] += params['limit']
                    break  # 成功获取数据，跳出重试循环
                    
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    print(f"  {species_name} 请求出错 ({retry_count}/{RETRY_COUNT}): {e}. {RETRY_DELAY}秒后重试...")
                    time.sleep(RETRY_DELAY)  # 出错时等待
            
            if retry_count == RETRY_COUNT:
                print(f"  {species_name}: 达到最大重试次数，跳过当前页")
            
            # 如果最后一页没有内容或者达到记录上限，退出
            if 'results' not in data or not data['results'] or len(results) < params['limit'] or record_count >= MAX_RECORDS_PER_SPECIES or record_count >= total_records:
                break
        
        # 保存结果到JSON文件
        if all_results:
            with open(species_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)
            
            print(f"  完成: {species_name} - 共 {record_count} 条记录")
            return record_count
        else:
            print(f"  警告: {species_name} 没有获取到任何记录")
            return 0
            
    except Exception as e:
        print(f"下载 {species_name} 时出错: {e}")
        return 0

def verify_json_files():
    """检查JSON文件是否有效，移除损坏的文件"""
    print("检查已下载的JSON文件...")
    invalid_files = []
    
    if not os.path.exists('occurrences'):
        return 0
        
    json_files = [f for f in os.listdir('occurrences') if f.endswith('.json')]
    for json_file in json_files:
        file_path = os.path.join('occurrences', json_file)
        try:
            # 尝试加载JSON文件验证其完整性
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # 检查是否为空数组或无效内容
            if not isinstance(data, list):
                invalid_files.append(file_path)
            elif len(data) == 0:
                # 空数组是有效的，但可能需要重新下载
                if DEBUG_MODE:
                    print(f"警告: {json_file} 是空数组")
        except (json.JSONDecodeError, UnicodeDecodeError):
            invalid_files.append(file_path)
    
    # 删除损坏的文件
    for invalid_file in invalid_files:
        print(f"删除损坏的文件: {os.path.basename(invalid_file)}")
        os.remove(invalid_file)
    
    return len(invalid_files)

def main():
    # 检查和清理损坏的JSON文件
    invalid_count = verify_json_files()
    if invalid_count > 0:
        print(f"已删除 {invalid_count} 个损坏的JSON文件")
    
    # 加载物种列表
    try:
        df = pd.read_csv('species_list_483.csv')
        species_list = df['scientific_name'].tolist()
        print(f"已加载 {len(species_list)} 个物种名称")
    except Exception as e:
        print(f"加载物种列表时出错: {e}")
        return
    
    # 记录开始时间
    start_time = datetime.now()
    print(f"开始下载，时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"启动 {PARALLEL_DOWNLOADS} 个并行下载进程...")
    
    # 统计数据
    total_downloaded = 0
    completed_species = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_DOWNLOADS) as executor:
        # 为每个物种创建任务
        futures = {
            executor.submit(download_species, species, i+1, len(species_list)): species
            for i, species in enumerate(species_list)
        }
        
        # 处理结果
        for future in concurrent.futures.as_completed(futures):
            species = futures[future]
            try:
                count = future.result()
                total_downloaded += count
                completed_species += 1
                
                # 每完成10个物种显示进度
                if completed_species % 10 == 0:
                    elapsed = datetime.now() - start_time
                    species_per_second = completed_species / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                    estimated_remaining = (len(species_list) - completed_species) / species_per_second if species_per_second > 0 else 0
                    
                    print(f"进度: {completed_species}/{len(species_list)} 物种 ({round(completed_species/len(species_list)*100, 2)}%)")
                    print(f"预计剩余时间: {round(estimated_remaining/60, 1)} 分钟")
            except Exception as e:
                print(f"处理物种 {species} 时出错: {e}")
    
    # 记录下载完成时间
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"下载完成！共下载 {total_downloaded} 条记录 / {completed_species} 个物种")
    print(f"总用时: {duration}")

if __name__ == "__main__":
    main()