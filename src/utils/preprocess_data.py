import ast
import logging
import re
from tqdm import tqdm

non_keywords = {'404 Not Found', '版权所有', '无法访问', '联系我们', '门户网站', '站点不存在', '公网安备', '网站地图',
                '背景颜色', 'ICP备案', '无障碍',
                '微信扫一扫', '打印本页', '分享微信', '浏览器推荐', '首页', '登录', '智能引导', '智能问答', '站群导航',
                '政务机器人', '邮件地址',
                'index.html', '动态信息', '404页面', '极速模式', '黔南州人民政府网站', 'My97DatePicker',
                '版权声明', '信息公开指南', '扫一下', '索引号', '邮政编码', '关怀版', '老年模式', '公安网备', '导航区',
                '扫描二维码',
                '长辈版', '运维电话', '网站标识码', '无法处理此请求', '网站标识码', '运维单位', '联系电话', '友情链接',
                '相关新闻', '上一篇', '下一篇', '360浏览器', '相关链接', '扫一扫', '县、市、区政府网站', '技术支持',
                'PlayPauseSeek',
                '关闭', '天气预报', '县内网站', '网站管理员', '网站管理电话', '县区网站', '省直部门网站',
                '国务院小程序',
                '县区政府', '国务院小程序', '中央部委网站', '相关网站', 'E-Mail', 'E-mail', '上一条', '下一条',
                '办公地点', '工作时间',
                '验证码', '传真', '社会主义核心价值观：', '电话：', "var d", 'var day', '机构设置政协提案',
                '北京天津河北山西内蒙古',
                '索 引 号', '搜一下', '本站检索', '站群检索', '信息来源', '日 一 二 三 四 五 六', '发布时间',
                '雅安领导', '重点领域公开专栏',
                '历史搜索', '工作报告政府公报', '公共企事业单位信息', '热搜词：', '电话号码：', '搜索范围', '热门搜索',
                '政府会议政策解读',
                '栏目内搜索', '12345政务服务热线', 'QQ空间', '浏览次数', '保护视力色', "The People's Government",
                '全方位建设模范自治区', '12345信箱', '全文下载', '政策咨询服务', '网上咨询|网上投诉', '快讯'
                }

def preprocess_data(content_df, max_length=1500):
    """
        处理输入的 DataFrame，提取 content 列数据并进行噪音检测，生成结果的 CSV 文件
        :param max_length:
        :param content_df: 输入的 DataFrame，包含 content 列
        :return: 处理后的 DataFrame 或 None
    """
    # 定义一个正则表达式，匹配中文字符
    chinese_pattern = re.compile(r'[\u4e00-\u9fa5]')
    # 定义地点关键词列表，去除无用句子
    location_keywords = ["网", "局", "厅", "部", "省", "办", "委", '县', '区', '院']

    progress_bar = tqdm(content_df.iterrows(), total=content_df.shape[0], desc="清洗数据")
    # 遍历 DataFrame 的每一行
    for idx, row in progress_bar:
        # 将字符串解析为 Python 对象
        content_list = is_list_format(row['content'])

        # # 如果 content_list 中包含多个列表，则将它们合并为一个列表
        # if any(is_list_format(i) for i in content_list):
        #     content_list = [item for sublist in content_list if is_list_format(sublist) for item in
        #                     ast.literal_eval(sublist)]
        unique_sentences = []
        total_length = 0

        if not isinstance(content_list, list):
            logging.warning(f"Row {idx} content is not a list format, skip it.")
            continue

        # 遍历嵌套列表中的每个子列表
        for sentence in content_list:
            # 如果句子不包含中文字符，则直接跳过
            if not re.search(chinese_pattern, sentence):
                continue

            # 计算单个关键词出现频率大于阈值的次数
            single_keyword_count = sum(sentence.count(keyword) >= 7 for keyword in location_keywords)
            # 如果句子中包含多个"人民政府"或"开发区"，则跳过该句子
            conditions = [
                sentence.count("人民政府") > 5,
                sentence.count("开发区") > 5,
                sum(sentence.count(keyword) for keyword in location_keywords) >= 20,
                single_keyword_count >= 1,
                sentence.count("年") > 7,
                sentence.count("》") > 7
            ]

            if len(sentence) < 200 and (any(conditions) or any(nk in sentence for nk in non_keywords)):
                continue

            # 提取前30个字符，去除重复的句子
            first_30_chars = sentence[5:30]
            is_duplicate = any(first_30_chars in s['text'][:40] for s in unique_sentences)
            if not is_duplicate:
                # 如果不是重复的句子且句子长度小于等于最大长度，则将其添加到列表中
                if total_length + len(sentence) <= max_length:
                    unique_sentences.append({'text': sentence, 'length': len(sentence)})
                    total_length += len(sentence)
            else:
                # 如果是重复的句子，则更新列表中已有的句子为最长的那个
                index = next((i for i, s in enumerate(unique_sentences) if s['text'][:30] == first_30_chars), None)
                if index is not None and len(sentence) > unique_sentences[index]['length']:
                    total_length -= unique_sentences[index]['length']
                    unique_sentences[index] = {'text': sentence, 'length': len(sentence)}
                    total_length += len(sentence)

        # 将所有唯一句子组合成一个字符串
        combined_content = '。'.join(s['text'] for s in unique_sentences)

        # 将处理后的字符串填入对应的 DataFrame 项中
        content_df.at[idx, 'content'] = combined_content

    return content_df


def is_list_format(string):
    try:
        # 尝试将字符串解析为 Python 对象
        result = ast.literal_eval(string)
        # 检查解析结果是否是 list
        return result
    except (ValueError, SyntaxError):
        # 如果解析失败，说明不是有效的 Python 表达式，或者不是 list 格式
        return False


def is_position_sensitive(news_text):

    # 检查全文是否包含 "市长" 或 "省长"
    if "市长" in news_text:
        return True
    if "省长" in news_text:
        return True

    # 将新闻文本按照句子进行分割
    sentences = re.split('[。！？；\n]', news_text)

    # 遍历每个句子
    for sentence in sentences:
        sentence = sentence.strip()

        # 检查 "市委" 和 "书记"
        if "市委" in sentence and "书记" in sentence:
            shiwei_positions = [m.start() for m in re.finditer("市委", sentence)]
            shuji_positions = [m.start() for m in re.finditer("书记", sentence)]
            for pos_shiwei in shiwei_positions:
                for pos_shuji in shuji_positions:
                    end_pos_shiwei = pos_shiwei + len("市委")
                    end_pos_shuji = pos_shuji + len("书记")
                    if pos_shiwei <= pos_shuji:
                        distance = pos_shuji - end_pos_shiwei
                    else:
                        distance = pos_shiwei - end_pos_shuji
                    if distance <= 5:
                        return True

        # 检查 "省委" 和 "书记"
        if "省委" in sentence and "书记" in sentence:
            shengwei_positions = [m.start() for m in re.finditer("省委", sentence)]
            shuji_positions = [m.start() for m in re.finditer("书记", sentence)]
            for pos_shengwei in shengwei_positions:
                for pos_shuji in shuji_positions:
                    end_pos_shengwei = pos_shengwei + len("省委")
                    end_pos_shuji = pos_shuji + len("书记")
                    if pos_shengwei <= pos_shuji:
                        distance = pos_shuji - end_pos_shengwei
                    else:
                        distance = pos_shengwei - end_pos_shuji
                    if distance <= 5:
                        return True
    return False

def is_position_sensitive_cities(news_text):
    if not isinstance(news_text, str):
        return False

    # 检查全文是否包含 "市长"
    if "市长" in news_text:
        return True

    # 将新闻文本按照句子进行分割
    sentences = re.split('[。！？；\n]', news_text)

    # 遍历每个句子
    for sentence in sentences:
        sentence = sentence.strip()

        # 检查 "市委" 和 "书记"
        if "市委" in sentence and "书记" in sentence:
            shiwei_positions = [m.start() for m in re.finditer("市委", sentence)]
            shuji_positions = [m.start() for m in re.finditer("书记", sentence)]
            for pos_shiwei in shiwei_positions:
                for pos_shuji in shuji_positions:
                    end_pos_shiwei = pos_shiwei + len("市委")
                    end_pos_shuji = pos_shuji + len("书记")
                    if pos_shiwei <= pos_shuji:
                        distance = pos_shuji - end_pos_shiwei
                    else:
                        distance = pos_shiwei - end_pos_shuji
                    if distance <= 5:
                        return True

    return False


def process_content(x):
    # 如果长度已经超过1500，则直接返回
    if len(x) > 1500:
        return x

    combined = []
    current_length = 0

    for sentence in x:
        if current_length + len(sentence) <= 1000:
            combined.append(sentence)
            current_length += len(sentence)
        else:
            break  # 达到最大长度，停止合并

    return ','.join(combined)  # 返回合并后的字符串

