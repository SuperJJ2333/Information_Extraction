import ast

import pandas as pd
import json

from utils.preprocess_data import preprocess_data

# 读取Excel文件
file_path = '../../temp/党政领导学习考察20240721.xlsx'
df = pd.read_excel(file_path)
# 处理content列
df = preprocess_data(df)

# 定义要提取的列
content_column = "content"
info_columns = [
    "来访城市", "受访城市", "来访人姓名", "来访人职位",
    "接访人姓名", "接访人职位", "访问年份", "访问时间",
    "访问天数", "主要交流内容", "是否属于数字政府议题", "政策议题"
]


# 准备输出文件
output_file = '../fine_tune/output.jsonl'
with open(output_file, 'w', encoding='utf-8') as f_out:
    for index, row in df.iterrows():

        # 提取content列并生成prompt
        content = row[content_column]
        date = row['date']
        topic = row['topic']
        prompt = {
            "messages": [
                {
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
                },
                {
                    "role": "user",
                    "content": f"请提取以下“学习考察”新闻的信息，返回符合要求的 JSON 格式数据。\n"
                               f"新闻标题为：{topic}\n新闻发布时间为：{date}\n"
                               f"新闻内容如下：{content}"
                }
            ]
        }

        # 提取其他列信息
        assistant_content = {}
        for col in info_columns:
            value = str(row[col]).replace('\n', ',') if pd.notna(row[col]) else ""
            assistant_content[col] = value

        # 创建一个新的字典来保持顺序
        new_assistant_content = {}
        for key in assistant_content:
            if key == "主要交流内容":
                new_assistant_content["新闻摘要"] = assistant_content[key]
            else:
                new_assistant_content[key] = assistant_content[key]

        # 生成assistant的回答
        response = {
            "role": "assistant",
            "content": f"{json.dumps(new_assistant_content, ensure_ascii=False)}"  # 直接保留字典形式
        }

        # 生成最终的jsonl格式
        jsonl_data = json.dumps({"messages": prompt["messages"] + [response]}, ensure_ascii=False)
        f_out.write(jsonl_data + '\n')

print(f"JSONL数据已保存至 {output_file}")
