import os
import pandas as pd
import shutil


def organize_csv_by_province(directory):
    """
    遍历指定目录下的所有 CSV 文件，并根据 `province` 列的内容，
    将文件移动到对应的子文件夹中。子文件夹名称为 `province` 的名称。

    参数:
    directory (str): 目标路径
    """
    # 检查路径是否存在
    if not os.path.exists(directory):
        print(f"路径 {directory} 不存在。")
        return

    # 遍历目录下所有文件
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)

                try:
                    # 读取 CSV 文件
                    df = pd.read_csv(file_path)

                    # 检查是否存在 `province` 列
                    if 'province' in df.columns:
                        # 获取 `province` 列的第一个值
                        province_name = df['province'].iloc[0]

                        # 创建省份文件夹路径
                        province_folder = os.path.join(directory, province_name)

                        # 如果文件夹不存在，则创建它
                        if not os.path.exists(province_folder):
                            os.makedirs(province_folder)

                        # 将文件移动到相应的省份文件夹
                        shutil.move(file_path, os.path.join(province_folder, file))
                        print(f"已将文件 {file} 移动到文件夹 {province_folder}")
                    else:
                        print(f"文件 {file} 不包含 `province` 列")

                except Exception as e:
                    print(f"处理文件 {file} 时发生错误: {e}")


# 示例调用
directory_path = '../../processed_data/Total_time_classified_data'
organize_csv_by_province(directory_path)
