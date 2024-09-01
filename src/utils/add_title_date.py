import os
import pandas as pd


def process_and_merge_files(origin_path, classified_path):
    """
    处理origin_path下的所有xlsx文件，并将其中的province, topic, date列复制到
    classified_path下匹配的csv文件中，最终保存文件并按指定列排序。

    :param origin_path: 包含xlsx文件的源文件夹路径
    :param classified_path: 目标文件夹路径，包含要更新的csv文件
    """
    # 获取origin_path下所有的xlsx文件
    for filename in os.listdir(origin_path):
        if filename.endswith('.xlsx'):
            # 提取文件名的split('_')[0]部分
            match_name = filename.split('_')[0]
            xlsx_file_path = os.path.join(origin_path, filename)

            # 遍历classified_path下所有csv文件，寻找包含match_name的文件
            for csv_filename in os.listdir(classified_path):
                if csv_filename.endswith('.csv') and match_name in csv_filename:
                    csv_file_path = os.path.join(classified_path, csv_filename)
                    print(f"Processing {xlsx_file_path} and {csv_file_path}")

                    # 读取xlsx文件
                    xlsx_df = pd.read_excel(xlsx_file_path)
                    # 读取csv文件
                    csv_df = pd.read_csv(csv_file_path)

                    # 确保xlsx文件包含所需的列
                    if all(col in xlsx_df.columns for col in ['province', 'topic', 'date']):
                        # 将province, topic, date列复制到csv DataFrame中
                        csv_df['province'] = xlsx_df['province']
                        csv_df['topic'] = xlsx_df['topic']
                        csv_df['date'] = xlsx_df['date']

                        # 确保csv文件包含的其他列
                        if all(col in csv_df.columns for col in ['content', 'label', 'score']):
                            # 按指定列排序
                            csv_df = csv_df[['province', 'topic', 'date', 'content', 'label', 'score']]

                            # 保存更新后的csv文件
                            csv_df.to_csv(csv_file_path, index=False)
                            print(f"Updated and saved: {csv_file_path}")
                        else:
                            print(f"Error: Missing required columns in {csv_file_path}. Skipping.")
                    else:
                        print(f"Error: Missing required columns in {xlsx_file_path}. Skipping.")
                else:
                    pass
                    # print(f"No matching CSV file found for {filename} in classified_path.")


if __name__ == '__main__':
    # 示例调用
    origin_path = "../../data/province_web_data"
    classified_path = "../../processed_data"
    process_and_merge_files(origin_path, classified_path)
