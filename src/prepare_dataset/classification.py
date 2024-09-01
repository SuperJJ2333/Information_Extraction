from paddlenlp import Taskflow
from pprint import pprint
import pandas as pd
import ast

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class Classification:
    def __init__(self, files_path):
        self.files_path = files_path
        self.cls = Taskflow("zero_shot_text_classification", schema=["学习考察", "访问学习", "其他"],
                            batch_size=32, max_seq_len=2048)

    def load_xlsx(self):
        """
        读取Excel文件中的数据
        转换'content'列中的字符串表示的列表为实际的列表，然后合并为逗号分隔的字符串
        """
        self.original_data = pd.read_excel(self.files_path, sheet_name='Sheet2')
        self.data = self.original_data.copy()

        self.data['content'] = self.data['content'].apply(ast.literal_eval)
        self.data['content'] = self.data['content'].apply(lambda x: ','.join(x))

    def text_classification(self):
        """
        文本分类
        """
        cls_input = [[content] for content in self.data['content']]
        temp = self.cls(cls_input)
        pprint(temp)


if __name__ == '__main__':
    path = r'../../temp/test_dataset.xlsx'
    method = Classification(path)
    method.load_xlsx()
    method.text_classification()
