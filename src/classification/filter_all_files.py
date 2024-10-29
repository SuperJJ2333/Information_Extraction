import os
import pandas as pd
import json

from utils.preprocess_data import *

"""
筛选包含“市长”的新闻内容，并保存筛选结果到对应的输出文件夹中。
输入文件夹：../../processed_data/Total_time_classified_data

并且保留原始数据中的所有行
"""
def process_csv_files(folder_path):
    """
        处理指定文件夹下的所有 CSV 文件，筛选包含“市长”的新闻内容，并保存筛选结果到对应的输出文件夹中。

        :param folder_path: 输入文件夹路径
    """
    base_output_folder = '../../all_processed_data'

    # 获取输入文件夹的相对路径（用于在输出文件夹中创建相同的子目录结构）
    relative_base_path = os.path.relpath(folder_path, '../../processed_data')

    # 构建输出文件夹路径
    output_folder = os.path.join(base_output_folder, relative_base_path)
    os.makedirs(output_folder, exist_ok=True)

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)
                file_name = os.path.basename(file_path)
                print(f'正在处理文件 {file_name}')

                # 生成筛选结果文件名
                filtered_file_name = f"{os.path.splitext(file_name)[0]}_整体结果.csv"

                # 计算当前文件所在子目录相对于输入文件夹的路径
                relative_subdir = os.path.relpath(root, folder_path)

                # 构建输出子目录路径
                target_subdir = os.path.join(output_folder, relative_subdir)
                os.makedirs(target_subdir, exist_ok=True)

                # 构建筛选结果文件的完整路径
                filtered_file_path = os.path.join(target_subdir, filtered_file_name)

                df['content'] = df['content'].fillna('')
                # Apply function
                df['is_match'] = df['content'].apply(is_position_sensitive_cities)

                # Filter if necessary
                df['是否为“学习考察”新闻'] = df['is_match'].apply(lambda x: "是" if x else "否")
                filtered_df = df
                df['content'] = df['content'].astype(str)

                if not filtered_df.empty:
                    filtered_df.to_csv(filtered_file_path, index=False, encoding='utf-8-sig')
                    print(f'文件 {file_name} 已保存为 {filtered_file_name}')
                    print(f'原有新闻共 {len(df)} 条，筛选后共 {len(filtered_df)} 条')


def process_province_csv_files(folder_path):
    """
        处理指定文件夹下的所有 CSV 文件，筛选包含“市长” or "省长"的新闻内容，并保存筛选结果到对应的输出文件夹中。

        :param folder_path: 输入文件夹路径
    """
    base_output_folder = '../../all_processed_data'

    # 获取输入文件夹的相对路径（用于在输出文件夹中创建相同的子目录结构）
    relative_base_path = os.path.relpath(folder_path, '../../processed_data')

    # 构建输出文件夹路径
    output_folder = os.path.join(base_output_folder, relative_base_path)
    os.makedirs(output_folder, exist_ok=True)

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)
                file_name = os.path.basename(file_path)
                print(f'正在处理文件 {file_name}')

                # 生成筛选结果文件名
                filtered_file_name = f"{os.path.splitext(file_name)[0]}_整体结果.csv"

                # 计算当前文件所在子目录相对于输入文件夹的路径
                relative_subdir = os.path.relpath(root, folder_path)

                # 构建输出子目录路径
                target_subdir = os.path.join(output_folder, relative_subdir)
                os.makedirs(target_subdir, exist_ok=True)

                # 构建筛选结果文件的完整路径
                filtered_file_path = os.path.join(target_subdir, filtered_file_name)

                df['content'] = df['content'].fillna('')
                # Apply function
                df['is_match'] = df['content'].apply(is_position_sensitive)

                # Filter if necessary
                df['是否为“学习考察”新闻'] = df['is_match'].apply(lambda x: "是" if x else "否")
                filtered_df = df
                df['content'] = df['content'].astype(str)

                if not filtered_df.empty:
                    filtered_df.to_csv(filtered_file_path, index=False, encoding='utf-8-sig')
                    print(f'文件 {file_name} 已保存为 {filtered_file_name}')
                    print(f'原有新闻共 {len(df)} 条，筛选后共 {len(filtered_df)} 条')


# Example usage
# process_csv_files('path_to_your_folder')
if __name__ == '__main__':
    process_csv_files('../../processed_data/Total_time_classified_data')
    process_province_csv_files('../../processed_data/province_classified_data')
