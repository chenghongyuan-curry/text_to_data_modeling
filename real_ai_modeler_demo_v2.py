import os
import json
from google import genai


# 配置 API Key
os.environ["GEMINI_API_KEY"] = 'AIzaSyDDRRw2rxrGx49DXUHNbhHTY8kgLJco7-Q'
def enum_ai_modeler(user_prompt, meta_json):
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

    system_instruction = """
    你是一个顶级的数仓建模专家。你生成的代码必须严格遵守以下【生产环境 DWD 规范】：

    1. 【元数据驱动转换 (核心要求)】：
       - 检查 JSON 元数据中字段的 `mapping` 属性。
       - 如果字段定义了 `mapping`，在生成 SQL 时必须使用 `CASE WHEN` 语句进行转换。
       - 例如：`CASE status WHEN 'DONE' THEN '已完成' ... ELSE status END AS po_status`。

    2. 【SQL 结构规范】：
       - 使用 `INSERT OVERWRITE TABLE ... PARTITION(pt='${bizdate}')`。
       - 采用【嵌套子查询】结构：在子查询中进行 `CASE WHEN` 转换和 `is_deleted=0` 过滤。
       - 字段投影按业务模块分组并添加注释。

    3. 【DDL 规范】：
       - 严格对齐用户提供的 DWD 模板（BIGINT, DOUBLE, DATETIME, AliOrcSerDe, LIFECYCLE 7）。
    """

    prompt = f"""
    【元数据约定 (JSON)】:
    {meta_json}

    【用户需求】:
    {user_prompt}

    请生成包含【状态枚举转换】逻辑的 DDL 和 ETL SQL。
    """

    response = client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=prompt,
        config={
            "system_instruction": system_instruction,
            "temperature": 0
        }
    )

    return response.text


if __name__ == "__main__":
    # 加载包含枚举映射的元数据
    with open('/Users/chenghongyuan/PycharmProjects/MyFirstPython/text_to_data_modeling/real_meta_config_v2.json', 'r') as f:
        meta = f.read()

    user_query = "构建采购订单明细轻量聚合表。要求：关联订单行、订单头和用户表,购买人需要关联名称和邮箱。特别注意：需要根据元数据中的映射关系，将订单状态(status)转换成中文描述。"

    if not os.environ.get("GEMINI_API_KEY"):
        print("请先设置 GEMINI_API_KEY")
    else:
        print("🛠️ 正在生成包含复杂枚举转换逻辑的代码...")
        print(enum_ai_modeler(user_query, meta))
