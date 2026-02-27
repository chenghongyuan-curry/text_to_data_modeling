import os
import json
from google import genai

# 配置 API Key
os.environ["GEMINI_API_KEY"] = 'AIzaSyDDRRw2rxrGx49DXUHNbhHTY8kgLJco7-Q'

class ModularDWEngine:
    def __init__(self):
        self.client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
        self.model = "gemini-3-flash-preview"

    def analyze_requirement(self, user_query, meta_json):
        """
        需求分析：识别需求类型（明细/汇总）并拆解任务
        """
        system_instruction = """
        你是一个数仓专家。请分析用户需求并输出 JSON 格式的任务拆解：
        {
          "requirement_type": "DETAIL" | "SUMMARY",
          "target_table": "目标表名",
          "involved_tables": ["表1", "表2"],
          "key_metrics": ["指标1"],
          "logic_steps": ["步骤1", "步骤2"]
        }
        """
        response = self.client.models.generate_content(
            model=self.model,
            contents=f"需求: {user_query}\n元数据: {meta_json}",
            config={"system_instruction": system_instruction, "response_mime_type": "application/json"}
        )
        return json.loads(response.text)

    def generate_ods_layer(self, table_meta):
        """
        接入层规则：生成 DataX JSON 脚本 + 类型映射
        """
        system_instruction = """
        你是一个数据集成专家。请为给定的表生成 DataX JSON 配置脚本。
        规则：
        1. 配置 MysqlReader 和 HdfsWriter。
        2. 必须包含字段类型映射（如 MySQL varchar -> Hive string）。
        3. 输出标准的 DataX JSON 格式。
        """
        response = self.client.models.generate_content(
            model=self.model,
            contents=f"表元数据: {json.dumps(table_meta)}",
            config={"system_instruction": system_instruction}
        )
        return response.text

    def generate_dwd_layer(self, table_meta, mapping_rules):
        """
        模型层规则：枚举转换、ID/Code 关联、逻辑删除过滤
        """
        system_instruction = """
        你是一个数仓建模专家。请生成 DWD 层代码。
        规则：
        1. 必须处理枚举映射（CASE WHEN）。
        2. 必须处理 ID 到 Code 的关联转换。
        3. 必须过滤 is_deleted=0 和 pt='${bizdate}'。
        4. 采用嵌套子查询风格。
        """
        content = f"元数据: {json.dumps(table_meta)}\n映射规则: {json.dumps(mapping_rules)}"
        response = self.client.models.generate_content(
            model=self.model,
            contents=content,
            config={"system_instruction": system_instruction}
        )
        return response.text

    def generate_ads_layer(self, req_analysis, dwd_context):
        """
        应用层规则：智能判断汇总或明细需求
        """
        system_instruction = """
        你是一个 BI 开发专家。
        规则：
        1. 如果需求类型是 SUMMARY，生成包含 GROUP BY 的聚合 SQL。
        2. 如果需求类型是 DETAIL，生成多模型关联的明细宽表 SQL。
        3. 结果必须直接面向业务报表。
        """
        content = f"需求分析: {json.dumps(req_analysis)}\nDWD背景: {dwd_context}"
        response = self.client.models.generate_content(
            model=self.model,
            contents=content,
            config={"system_instruction": system_instruction}
        )
        return response.text


if __name__ == "__main__":
    engine = ModularDWEngine()

    # 示例元数据
    with open('/Users/chenghongyuan/PycharmProjects/MyFirstPython/text_to_data_modeling/real_meta_config_v2.json', 'r') as f:
        meta = json.load(f)

    user_query = "我要做一个采购订单明细报表，展示订单行信息、采购员姓名，并把状态转为中文。"

    if not os.environ.get("GEMINI_API_KEY"):
        print("请先设置 GEMINI_API_KEY")
    else:
        print("🚀 启动模块化数仓流水线...")

        # 1. 需求分析
        analysis = engine.analyze_requirement(user_query, json.dumps(meta))
        print(f"\n[需求分析结果]: {analysis['requirement_type']} 需求")

        # 2. 接入层 (以第一个表为例)
        print("\n[接入层] 正在生成 DataX 脚本...")
        ods_script = engine.generate_ods_layer(meta['tables'][0])
        print(ods_script[:300] + "...")  # 截断展示

        # 3. 模型层
        print("\n[模型层] 正在生成 DWD 转换逻辑...")
        dwd_code = engine.generate_dwd_layer(meta['tables'], "枚举映射+ID关联")
        print(dwd_code)

        # 4. 应用层
        print("\n[应用层] 正在生成最终报表 SQL...")
        ads_code = engine.generate_ads_layer(analysis, dwd_code)
        print(ads_code)
