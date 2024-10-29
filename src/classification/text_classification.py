import logging
import os
import re
from pprint import pprint
from paddlenlp import Taskflow
import paddle
import pandas as pd
import ast
from datetime import datetime
import warnings
from tqdm import tqdm

from src.utils.preprocess_data import preprocess_data

warnings.simplefilter(action='ignore', category=FutureWarning)
# 创建日志器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建格式化器并将其添加到处理器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# 将处理器添加到日志器
logger.addHandler(console_handler)


class TextClassification:
    """
    对data中的数据进行分类
    分类结果保存到processed_data中

    作用：
    1. 识别“政府领导到其他城市考察学习或访问”、“不属于政府领导到其他城市考察学习的新闻”、“其他”这三个类别
    2. 给出每个类别的概率
    3. 求出每个类别的平均分
    """
    def __init__(self):
        self.folder_path = "../../data/Total_time_range_data"
        self.save_path = "../../processed_data/Total_time_classified_data"
        self.dataframe_dict = {}
        self.loop_limit = 3

        self.schema_list = ["政府领导到其他城市考察学习或访问", "不属于政府领导到其他城市考察学习的新闻", "其他"]
        # 创建分类器
        self.cls = Taskflow("zero_shot_text_classification", schema=self.schema_list, batch_size=128, max_seq_len=1024,
                            precision='fp32', model='utc-xbase')

    def read_xlsx(self):
        """
        读取指定文件夹中的xlsx文件
        :return:
        """
        if not os.path.exists(self.folder_path):
            logging.error("无法找到文件夹: {}".format(self.folder_path))
            return False

        for root, dirs, files in os.walk(self.folder_path):

            last_folder_name = os.path.join(root)

            for filename in files:
                if filename.endswith('学习考察.xlsx'):
                    file_path = os.path.join(last_folder_name, filename)
                    logging.info("正在读取文件: {}".format(file_path))

                    df = pd.read_excel(file_path)
                    self.dataframe_dict[filename.split('.')[0]] = df

        logging.info("共读取到 {} 个文件".format(len(self.dataframe_dict)))

    def main_classify(self):
        """
        对所有已加载的数据框中的文本内容进行分类
        :return:
        """
        if not self.dataframe_dict:
            logging.error("没有可分类的数据")
            return False
        progress_bar = tqdm(self.dataframe_dict.items(), total=len(self.dataframe_dict),  desc="分类进度")

        # 设置进度条以跟踪分类进度
        for key, df in progress_bar:
            self.file_name = key
            logging.info("\n正在清洗: {}".format(self.file_name))

            # 预处理数据
            df = preprocess_data(df)
            # 分类数据
            classified_data = self.loop_classify(df)
            # 合并 df 和 classified_data
            classified_data = pd.DataFrame(classified_data)
            combined_data = pd.concat([df, classified_data], axis=1)
            # 保存结果
            self.save_to_csv(combined_data, key)

    def loop_classify(self, data):
        """
        使用多次迭代对内容列进行分类以提高可靠性
        :return:
        """

        # 存储最终结果
        final_results = []
        logging.info(f"{self.file_name} - 开始分类")

        for content in data['content']:
            if not content:
                continue

            label_counts = {label: 0 for label in self.schema_list}  # 用于记录每个label的出现次数
            label_total_scores = {label: 0 for label in self.schema_list}  # 用于记录每个label的最高score

            for _ in range(self.loop_limit):
                temp = self.cls(content)

                # 获取每一轮的预测结果
                predictions = temp[0].get('predictions', [])

                for prediction in predictions:
                    label = prediction.get('label')
                    score = prediction.get('score')

                    # 增加label的出现次数
                    label_counts[label] += 1

                    # 更新最高的score
                    label_total_scores[label] += score

            # 找出出现次数最多的label
            final_label = max(label_counts, key=label_counts.get)

            # 确保不会除以零
            if label_counts[final_label] > 0:
                final_score = label_total_scores[final_label] / label_counts[final_label]
            else:
                final_score = 0  # 或者设置为一个适当的默认值

            # 记录最终结果
            final_results.append({'content': content, 'label': final_label, 'score': final_score})

        return final_results

    def save_to_csv(self, final_results, file_name):
        df = final_results
        df.to_csv(os.path.join(self.save_path, f'{file_name}_分类结果.csv'), index=False)
        logging.info(f'保存结果到{file_name}_分类结果.csv')


if __name__ == '__main__':
    main = TextClassification()
    main.read_xlsx()
    main.main_classify()
