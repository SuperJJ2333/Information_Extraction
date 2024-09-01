import openai
import os

# 设置代理
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

# 设置 OpenAI API 密钥
openai.api_key = "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"

# 上传文件
try:
    # 上传文件
    with open("output.jsonl", "rb") as file:
        response = openai.File.create(
            file=file,
            purpose="fine-tune"
        )
        file_id = response['id']
        print(f"文件上传成功，文件ID: {file_id}")
except Exception as e:
    print(f"文件上传失败: {e}")
    exit()

# 检查文件上传状态
try:
    file_info = openai.File.retrieve(file_id)
    print(f"文件状态: {file_info['status']}")
except Exception as e:
    print(f"获取文件状态失败: {e}")
    exit()

# 创建微调任务
try:
    fine_tune_response = openai.FineTuningJob.create(
        training_file=file_id,
        model="gpt-4o-mini-2024-07-18"  # 指定模型为 4o-mini
    )
    fine_tune_id = fine_tune_response['id']
    print(f"微调任务已创建，任务ID: {fine_tune_id}")
except Exception as e:
    print(f"微调任务创建失败: {e}")
    exit()

# 获取微调任务状态
try:
    fine_tune_status = openai.FineTuningJob.retrieve(fine_tune_id)
    print(f"微调任务状态: {fine_tune_status['status']}")
except Exception as e:
    print(f"获取微调任务状态失败: {e}")
    exit()

# 监控微调任务进度（可选）
try:
    for event in openai.FineTune.list_events(id=fine_tune_id):
        print(f"微调事件: {event['message']}")
except Exception as e:
    print(f"获取微调事件失败: {e}")
