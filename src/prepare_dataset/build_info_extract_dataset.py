import ast

import pandas as pd
import json

from utils.preprocess_data import preprocess_data


def process_content(x):
    # 防止长度过长
    if len(x) > 1500:
        return x
    return ','.join(x)


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
output_file = 'info_extract_output.jsonl'
with open(output_file, 'w', encoding='utf-8') as f_out:
    for index, row in df.iterrows():
        # 提取content列并生成prompt
        content = row[content_column]
        prompt = {
            "messages": [
                {"role": "system", "content": "你是一个AI助理。"},
                {"role": "user", "content": f"""
【任务描述】
请从一段“学习考察”新闻文本中提取出以下相关信息：来访城市、受访城市、来访人姓名、来访人职位、接访人姓名、接访人职位、访问年份、访问时间、访问天数、主要交流内容、是否属于数字政府议题、政策议题相关的信息，并以JSON格式返回{{"来访城市": <来访城市>, "受访城市": <受访城市>, "来访人姓名": <来访人姓名>, "来访人职位": <来访人职位>, "接访人姓名": <接访人姓名>, "接访人职位": <接访人职位>, "访问年份": <访问年份>, "访问时间": <访问时间>, "访问天数": <访问天数>, "主要交流内容": <主要交流内容>, "是否属于数字政府议题": <是否属于数字政府议题>, "政策议题": <政策议题>}}。 请注意如果内容中没有明确信息，请在结果的相应字段返回空字符串。例如：{{"来访城市": "", "受访城市": "", "来访人姓名": "", "来访人职位": "", "接访人姓名": "", "接访人职位": "", "访问年份": "", "访问时间": "", "访问天数": "", "主要交流内容": "", "是否属于数字政府议题": "", "政策议题": ""}}
【待分析内容】
{content}
请根据【任务描述】针对【待分析内容】进行分析，给出结果。"""
                 }
            ]
        }

        # 提取其他列信息
        assistant_content = {}
        for col in info_columns:
            value = str(row[col]).replace('\n', ',') if pd.notna(row[col]) else ""
            assistant_content[col] = value

        # 生成assistant的回答
        response = {
            "role": "assistant",
            "content": json.dumps(assistant_content, ensure_ascii=False)
        }

        # 生成最终的jsonl格式
        jsonl_data = json.dumps({"messages": prompt["messages"] + [response]}, ensure_ascii=False)
        f_out.write(jsonl_data + '\n')

print(f"JSONL数据已保存至 {output_file}")
