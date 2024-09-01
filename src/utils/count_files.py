import os

def count_xlsx_files(directory, file_type='.xlsx'):
    """
    返回指定路径下所有 .xlsx 文件的数量。

    参数:
    directory (str): 目标路径

    返回:
    int: .xlsx 文件的数量
    """
    # 检查路径是否存在
    if not os.path.exists(directory):
        print(f"路径 {directory} 不存在。")
        return 0

    xlsx_count = 0

    # 使用 os.walk 遍历所有子文件夹
    for root, dirs, files in os.walk(directory):
        # 过滤出所有的 .xlsx 文件并计数
        xlsx_files = [file for file in files if file.endswith(file_type)]
        xlsx_count += len(xlsx_files)

    return xlsx_count


# 示例调用
directory_path = '../../processed_data/Total_time_classified_data'
original_path = '../../data/Total_time_range_data'
csv_count = count_xlsx_files(directory_path, '.csv')
xlsx_count = count_xlsx_files(original_path, '.xlsx')
print(f"该路径下有 {csv_count} 个 .csv 文件。")
print(f"该路径下有 {xlsx_count} 个 .xlsx 文件。")
