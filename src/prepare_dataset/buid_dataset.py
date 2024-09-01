import pandas as pd
import os
import ast
import logging

# 配置日志记录
logging.basicConfig(level=logging.WARNING)

class NewsAnalysis:
    def __init__(self, files_path: str):
        """
        初始化NewsAnalysis类

        :param files_path: Excel文件路径
        """
        self.files_path = files_path
        self.data = None
        self.max_relations_per_row = 15

    def read_data(self):
        """
        读取Excel文件中的数据
        """
        self.original_data = pd.read_excel(self.files_path, sheet_name='Sheet2')
        self.data = self.original_data.copy()

    def transform_content(self):
        """
        转换'content'列中的字符串表示的列表为实际的列表，然后合并为逗号分隔的字符串
        """
        self.data['content'] = self.data['content'].apply(ast.literal_eval)
        self.data['content'] = self.data['content'].apply(lambda x: ','.join(x))

    def analyze_positions(self):
        """
        分析职位并创建关系描述

        :return: 发现的关系集合
        """
        # 初始化存储分析结果的列表
        max_relations_per_row = self.max_relations_per_row
        for i in range(1, max_relations_per_row + 1):
            self.data[f'实体关系{i}'] = None  # 添加足够的列来存储关系

        # 初始化存储分析结果的列表
        previous_content = None  # 用于存储上一行的内容
        relation_set = set()

        # 遍历每一行
        for index, row in self.data.iterrows():
            content = row['content']
            if pd.isna(content) and previous_content is not None:
                # 如果当前行的content为空，则使用上一行的content
                content = previous_content
                logging.warning(f"第{index}行的内容为空，使用上一行的内容")
            elif pd.isna(content):
                # 如果连续行的content为空，则跳过
                continue
            relation_set = set()  # 清空关系集合
            entities = [
                ('来访城市', row.get('来访城市'), '来访人姓名', row.get('来访人姓名'), '来自'),
                ('来访人职位', row.get('来访人职位'), '来访人姓名', row.get('来访人姓名'), '担任'),
                ('受访城市', row.get('受访城市'), '接访人姓名', row.get('接访人姓名'), '来自'),
                ('接访人职位', row.get('接访人职位'), '接访人姓名', row.get('接访人姓名'), '担任'),
                ('来访人姓名', row.get('来访人姓名'), '接访人姓名', row.get('接访人姓名'), '接待')
            ]

            # 遍历实体对
            for entity1_type, entity1_value, entity2_type, entity2_value, relation in entities:
                if not pd.isna(entity2_value):
                    if "郑人豪" in entity2_value:
                        logging.warning(f"第{index}行的{entity2_type}:{entity2_value}为特殊值，跳过")

                if pd.isna(entity1_value) or pd.isna(entity2_value):
                    logging.warning(f"{entity1_type}:{entity1_value} - {entity2_type}:{entity2_value} - 数据中存在空值，请检查。")
                    continue

                # 处理特殊情况，如果实体值中包含换行符，则分割为多行
                if '\n' in entity1_value or '\n' in entity2_value:
                    temp_results = self.cut_off_data(entity1_type, entity1_value, entity2_value, relation, content, row)
                    relation_set.update(temp_results)
                else:
                    self.create_relation_desc(entity1_type, entity1_value, entity2_value, relation, content, row, relation_set)

            # 存储关系到列
            for i, relation in enumerate(relation_set):
                if i < max_relations_per_row:
                    self.data.at[index, f'实体关系{i + 1}'] = relation

            previous_content = content  # 更新上一行的内容为当前行

        return relation_set

    def cut_off_data(self, entity1_type, entity1_value, entity2_value, relation, content, row):
        """
        处理实体值中包含换行符的情况，分割实体值并构建关系描述。

        :param entity1_type: 实体1的类型
        :param entity1_value: 实体1的值
        :param entity2_value: 实体2的值
        :param relation: 实体之间的关系
        :param content: 文本内容
        :param row: 当前行数据
        :return: 包含关系描述的集合
        """
        # 初始化变量
        entity1_split_data = [entity1_value]
        entity2_split_data = [entity2_value]
        relation_set = set()

        # 如果实体1的值包含换行符，则分割实体1的值
        if '\n' in entity1_value:
            entity1_split_data = [i.strip() for i in entity1_value.split('\n') if i.strip() != '']

        # 如果实体2的值包含换行符，则分割实体2的值
        if '\n' in entity2_value:
            entity2_split_data = [i.strip() for i in entity2_value.split('\n') if i.strip() != '']

        # 如果分割后的实体1和实体2的数量相等，处理多对多情况
        if len(entity1_split_data) == len(entity2_split_data):
            # 处理多对多情况
            for e1_value, e2_value in zip(entity1_split_data, entity2_split_data):
                self.create_relation_desc(entity1_type, e1_value, e2_value, relation, content, row, relation_set)
        # 否则，处理一对多或多对一情况
        else:
            if len(entity1_split_data) < len(entity2_split_data):
                for e2_value in entity2_split_data:
                    for e1_value in entity1_split_data:
                        self.create_relation_desc(entity1_type, e1_value, e2_value, relation, content, row, relation_set)
            else:
                for e1_value in entity1_split_data:
                    for e2_value in entity2_split_data:
                        self.create_relation_desc(entity1_type, e1_value, e2_value, relation, content, row, relation_set)

        return relation_set

    def save_results(self, save_path=r'../../temp'):
        """
        保存分析结果到Excel文件

        :param save_path: 保存结果的路径
        """
        # 去除无用列
        self.data = self.data[['content'] + [f'实体关系{i + 1}' for i in range(self.max_relations_per_row)]]
        # 重命名列名
        self.data = self.data.rename(columns={'content': '文本内容'})

        # 保存分析结果
        self.data.to_excel(os.path.join(save_path, 'final_test_dataset.xlsx'), index=False)

        print(f"Analysis results saved to {os.path.join(save_path, 'final_test_dataset.xlsx')}")

    def create_relation_desc(self, entity1_type, entity1_value, entity2_value, relation, content, row, relation_set):
        """
        构建关系描述并添加到关系集合中

        :param entity1_type: 实体1的类型
        :param entity1_value: 实体1的值
        :param entity2_value: 实体2的值
        :param relation: 实体之间的关系
        :param content: 文本内容
        :param row: 当前行数据
        :param relation_set: 存储关系的集合
        """
        start_idx1 = None
        end_idx1 = None
        start_idx2 = None
        end_idx2 = None

        # 判断是否含有职位信息
        if '职位' in entity1_type and entity2_value in content:

            possible_positions = [entity1_value,
                                  self.clean_entity_value(entity1_value, row.get('受访城市', ''), row.get('来访城市', ''))]

            (start_idx1, end_idx1), entity1_value = self.find_closest_index(content, possible_positions, entity2_value)

        elif entity1_value in content and entity2_value in content:
            start_idx1, end_idx1 = self.create_index(entity1_value, content)

        elif entity2_value in content:
            entity1_value = entity1_value.replace('市', '').strip()
            start_idx1, end_idx1 = self.create_index(entity1_value, content) if entity1_value in content else None, None

        if start_idx1 and end_idx1 and entity1_value:
            start_idx2, end_idx2 = self.create_index(entity2_value, content)
        else:
            print(f"{entity1_value} - {entity2_value} - 未找到匹配的实体")

        if start_idx1 is None or end_idx1 is None or start_idx2 is None or end_idx2 is None:
            return relation_set

        relation_desc = f"{{[{start_idx1},{end_idx1}],{entity1_value}}},{{[{start_idx2},{end_idx2}],{entity2_value}}},{relation}"
        if relation_desc not in relation_set:
            relation_set.add(relation_desc)

        return relation_set

    @staticmethod
    def create_index(entity_value, content):
        """
        创建实体值在内容中的索引位置

        :param entity_value: 要在内容中查找的实体值
        :param content: 文本内容
        :return: 起始和结束索引位置的元组
        """
        try:
            start_idx = content.index(entity_value)
            end_idx = start_idx + len(entity_value) - 1
            return start_idx, end_idx
        except ValueError:
            return None, None

    @staticmethod
    def find_closest_index(content, position_names, target_name):
        """
        查找目标名称的最接近的职位索引

        :param content: 文本内容
        :param position_names: 要搜索的职位名称列表
        :param target_name: 要查找的目标名称
        :return: 最佳匹配的索引和最佳职位的元组
        """
        # 找到最接近的职位索引
        min_distance = float('inf')
        best_match = (-1, -1)  # 存储最佳匹配的起始和结束索引
        best_position = None
        for position in position_names:
            if position in content:
                pos_index = content.index(position)
                name_index = content.index(target_name)
                distance = abs(pos_index - name_index)
                if distance < min_distance:
                    min_distance = distance
                    start_idx = pos_index
                    end_idx = pos_index + len(position) - 1
                    best_match = (start_idx, end_idx)
                    best_position = position
        if best_match == (-1, -1):
            logging.warning(f"未找到匹配的职位: {position_names} 接近 {target_name}")

        return best_match, best_position


    @staticmethod
    # 假设entity1_value是一个字符串，我们需要从中移除'受访城市'或'来访城市'的值
    def clean_entity_value(entity1_value, visited_city, visiting_city):
        """
        假设entity1_value是一个字符串，移除'受访城市'或'来访城市'的值

        :param entity1_value: 实体1的值
        :param visited_city: 受访城市
        :param visiting_city: 来访城市
        :return: 清理后的实体1值
        """

        # 创建一个从'受访城市'和'来访城市'字段中得到的所有可能城市的列表
        cities_to_remove = (visited_city.split('\n') + visiting_city.split('\n'))

        # 移除entity1_value中所有出现的城市名称
        for city in cities_to_remove:
            city = city.strip()  # 去除可能的空格
            entity1_value = entity1_value.replace(city, '')

        if '黄' in entity1_value.strip():
            print(f"Warning: {entity1_value} is not a valid entity value")

        return entity1_value.strip()  # 最后再次去除前后的空格以清理结果


if __name__ == '__main__':
    path = r'../../temp/test_dataset.xlsx'

    # 示例用法
    news_analysis = NewsAnalysis(path)
    news_analysis.read_data()
    news_analysis.transform_content()
    results = news_analysis.analyze_positions()
    for result in results:
        print(result)
    news_analysis.save_results()