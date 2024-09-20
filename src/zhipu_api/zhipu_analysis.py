import logging
import os
import pandas as pd
import json
from zhipuai import ZhipuAI  # 假设 zhipuai 是你使用的模型 SDK
from concurrent.futures import ThreadPoolExecutor
from zhipuai.core._errors import APIRequestFailedError

from utils.preprocess_data import *

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


class NewsExtractor:
    def __init__(self, directory, api_key):
        self.directory = directory
        self.client = ZhipuAI(api_key=api_key)
        self.save_path = '../../analysis_data'

        self.info_columns = [
            "来访城市", "受访城市", "来访人姓名", "来访人职位",
            "接访人姓名", "接访人职位", "访问年份", "访问时间",
            "访问天数", "新闻摘要", "是否属于数字政府议题", "政策议题"
        ]
        self.system_message = {
            "role": "system",
            "content": (
                "你是一个信息提取模型，专门提取“学习考察”新闻中的相关信息。请按照以下要求返回 JSON 格式的数据：\n"
                "1. 必须包含以下预定义的字段，确保字段名和顺序固定：\n"
                "   - 来访城市\n"
                "   - 受访城市\n"
                "   - 来访人姓名\n"
                "   - 来访人职位\n"
                "   - 接访人姓名\n"
                "   - 接访人职位\n"
                "   - 访问年份\n"
                "   - 访问时间\n"
                "   - 访问天数\n"
                "   - 新闻摘要\n"
                "   - 是否属于数字政府议题\n"
                "   - 政策议题（可多选，选项包括：宏观经济、政府运作、卫生健康、农林牧渔、劳动就业、教育、环境保护、能源利用、流动人口、交通运输、司法犯罪、社会政策、区域规划、贸易金融、民族宗教、科技通信、土地、国际交流、文化旅游、应急事件、住房、财政、军队国防、国有企业、数字政府）\n"
                "2. 输出格式必须为 JSON，且字段顺序不可变。\n"
                "3. 如果没有相关信息，请在对应字段中返回空字符串（\"\"）。\n"
                "4. 确保输出的 JSON 数据结构完全符合要求，不要包含额外信息。"
            )
        }
        self.messages = [self.system_message]  # 初始的消息列表，包含 system_message
        logger.info("NewsExtractor 实例化成功")

    def read_files(self):
        """读取指定目录下所有 csv 文件"""
        files = [file for file in os.listdir(self.directory) if file.endswith('.csv')]
        logger.info(f"成功获取 {len(files)} 个文件")
        return files

    def generate_user_message(self, content, date, topic):
        """生成用户消息部分"""
        user_message = {
            "role": "user",
            "content": f"请提取以下“学习考察”新闻的信息，返回符合要求的 JSON 格式数据。\n"
                       f"新闻标题为：{topic}\n新闻发布时间为：{date}\n"
                       f"新闻内容如下：{content}"
        }
        return user_message

    def extract_info(self, response_content):
        """解析模型返回的 JSON 格式数据"""
        try:
            extracted_data = json.loads(response_content)
            return {key: extracted_data.get(key, "") for key in self.info_columns}
        except json.JSONDecodeError:
            logger.error(f"返回数据有误 - {response_content}")
            # 如果解析失败，返回空值字典
            return {key: "" for key in self.info_columns}

    def call_model(self):
        """调用模型并获取返回的应答"""
        try:
            response = self.client.chat.completions.create(
                model="glm-4-9b:1820610167:learing:kq3jri0r",
                messages=self.messages,
                top_p=0.7,
                temperature=0.2,
                max_tokens=1024,
                stream=False
            )
            return response.choices[0].message.content
        except APIRequestFailedError as e:
            # 在捕获到 API 请求失败时，记录错误并抛出异常
            logger.error(f"API请求失败: {e} - {self.messages}")
            return None  # 或者你可以选择抛出一个更具体的异常，或者处理后继续执行
        except Exception as e:
            # 捕获所有其他异常，并记录错误
            logger.error(f"调用模型时发生未预料的错误: {e}", exc_info=True)
            return None
        finally:
            if len(self.messages) > 2:  # 确保至少还有 system_message
                self.messages.pop()

    def process_single_file(self, file_path):
        """处理单个 csv 文件并保存提取的信息为 csv 文件"""
        try:
            df = pd.read_csv(file_path)
            df_copy = df.copy()

            # 过滤出“市长”或“书记”等职位的新闻
            df_copy['is_match'] = df_copy['content'].apply(is_position_sensitive)
            df_copy = df_copy[df_copy['is_match'] == True]

            # 提取文件名
            file_name = os.path.basename(file_path)

            # 确保 DataFrame 包含所有 info_columns 中定义的列
            for col in self.info_columns:
                if col not in df_copy.columns:
                    df_copy[col] = ""

            # 只处理符合条件的行
            df_filtered = df[df['label'] == '政府领导到其他城市考察学习或访问']

            logger.info(f"{file_path} 开始分析")

            # 分析单个文件下的每一行
            for index, row in df_filtered.iterrows():
                content = row.get('content', '')
                date = row.get('date', '')
                topic = row.get('topic', '')
                if content:
                    user_message = self.generate_user_message(content, date, topic)
                    self.messages.append(user_message)

                    # 调用模型并获取返回的应答
                    response_content = self.call_model()
                    if not response_content:
                        continue

                    # 提取信息
                    extracted_info = self.extract_info(response_content)

                    # 将提取的信息填入df_copy的对应行
                    for key, value in extracted_info.items():
                        # 确保列存在
                        if key not in df_copy.columns:
                            df_copy[key] = None
                        df_copy.at[index, key] = value

            new_file_path = os.path.join(self.save_path, file_name)
            # 生成新文件名并保存新的 CSV 文件
            df_copy.to_csv(new_file_path, index=False, encoding='utf-8')
            logger.info(f"{new_file_path} 分析完成并保存")

        except APIRequestFailedError as e:
            # 处理特定的 API 请求失败错误
            logger.error(f"处理文件 {file_path} 时发生 API 请求失败错误: {e}")
        except Exception as e:
            # 捕获其他所有异常
            logger.error(f"处理文件 {file_path} 时发生错误: {e}", exc_info=True)

    def process_all_files(self):
        """处理所有文件并保存提取的信息为新 csv 文件"""
        files = self.read_files()

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(self.process_single_file, os.path.join(self.directory, file)) for file in files]
            for future in futures:
                try:
                    future.result()  # 获取处理结果，捕捉可能的异常
                except Exception as e:
                    logger.error(f"处理文件时发生错误: {e}", exc_info=True)


if __name__ == '__main__':
    # 使用示例
    directory = "../../test_data"
    api_key = "b912fa2024498bafb9be7e013f421bb7.ejrJyroWo81LHPmQ"  # 替换为实际的 API key
    news_extractor = NewsExtractor(directory, api_key)
    news_extractor.process_all_files()
