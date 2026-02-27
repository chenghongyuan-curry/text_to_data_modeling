import os
import re
import json
import sqlglot
from enum import Enum
from typing import List, Optional, Type, Any, Dict
from pydantic import BaseModel
from openai import OpenAI
import google.generativeai as genai
from .executor import Executor

# 步骤 1: 导入“好味道”代码样本库
try:
    from .good_smell_templates import (
        DWD_WIDE_TABLE_TEMPLATE, 
        ADS_AGGREGATION_TEMPLATE,
        DWS_DETAIL_TEMPLATE,
        DATAX_TEMPLATE,
        CLICKHOUSE_DDL_TEMPLATE
    )
except ImportError:
    from good_smell_templates import (
        DWD_WIDE_TABLE_TEMPLATE, 
        ADS_AGGREGATION_TEMPLATE,
        DWS_DETAIL_TEMPLATE,
        DATAX_TEMPLATE,
        CLICKHOUSE_DDL_TEMPLATE
    )

class RequirementType(str, Enum):
    DETAIL = "DETAIL"
    SUMMARY = "SUMMARY"

class TablePlanEntry(BaseModel):
    table_name: str
    status: str
    source_tables: List[str] = []
    action_detail: str = ""

class RequirementAnalysis(BaseModel):
    requirement_type: RequirementType
    target_table: str
    is_new_table: bool
    involved_tables: List[str]
    layer_plan: Dict[str, List[TablePlanEntry]]
    key_metrics: List[str]
    logic_steps: List[str]

class AutoDWEngine:
    def __init__(self, api_key=None):
        self.provider = os.environ.get("AI_PROVIDER", "aliyun").lower()
        print(f"[INFO] Initializing AutoDWEngine, Provider: {self.provider.upper()}")
        if self.provider == "aliyun":
            self.api_key = api_key or os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("GEMINI_API_KEY")
            if not self.api_key:
                 raise ValueError("[ERROR] DASHSCOPE_API_KEY not found!")
            self.client = OpenAI(api_key=self.api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
            self.model = "qwen3-max"
        elif self.provider == "google":
            self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
            if not self.api_key:
                 raise ValueError("[ERROR] GEMINI_API_KEY not found!")
            self.client = genai.Client(api_key=self.api_key)
            self.model = "gemini-2.5-flash"
        else:
            raise ValueError(f"[ERROR] Unsupported AI_PROVIDER: {self.provider}.")
            
        # 初始化 Executor 用于真实的 ClickHouse 动态预检
        self.executor = Executor()

    def _call_ai(self, system_instruction: str, prompt: str, response_schema: Optional[Type[BaseModel]] = None, is_json: bool = False):
        if self.provider == "aliyun":
            return self._call_aliyun(system_instruction, prompt, response_schema, is_json)
        else:
            return self._call_google(system_instruction, prompt, response_schema, is_json)

    def _call_aliyun(self, system_instruction, prompt, response_schema, is_json):
        messages = [{"role": "system", "content": system_instruction}, {"role": "user", "content": prompt}]
        try:
            if response_schema:
                completion = self.client.beta.chat.completions.parse(model=self.model, messages=messages, response_format=response_schema, temperature=0)
                return completion.choices[0].message.parsed
            else:
                params = {"model": self.model, "messages": messages, "temperature": 0}
                if is_json:
                    params["response_format"] = {"type": "json_object"}
                completion = self.client.chat.completions.create(**params)
                return completion.choices[0].message.content
        except Exception as e:
            raise RuntimeError(f"[ERROR] Aliyun API call failed: {str(e)}")

    def _call_google(self, system_instruction, prompt, response_schema, is_json):
        config = {"system_instruction": system_instruction, "temperature": 0}
        if response_schema:
            config["response_mime_type"] = "application/json"
            config["response_schema"] = response_schema
        elif is_json:
            config["response_mime_type"] = "application/json"
        try:
            response = self.client.models.generate_content(model=self.model, contents=prompt, config=config)
            if response_schema:
                if hasattr(response, 'parsed') and response.parsed:
                    return response.parsed
                return response_schema.model_validate_json(response.text)
            return response.text
        except Exception as e:
            error_msg = str(e)
            if "NOT_FOUND" in error_msg or "404" in error_msg:
                print(f"[ERROR] Google model '{self.model}' not found or unavailable.")
                self._list_google_models()
            raise RuntimeError(f"[ERROR] Google API call failed: {error_msg}")

    def _list_google_models(self):
        print("[INFO] Fetching available Google models...")
        try:
            for model in self.client.models.list():
                if "generateContent" in getattr(model, 'supported_actions', []):
                    print(f" - {model.name}")
        except Exception as e:
            print(f"[WARNING] Failed to fetch model list: {e}")

    def _clean_sql_code(self, text: str) -> str:
        return text.replace("```sql", "").replace("```", "").strip()

    def _clean_json_code(self, text: str) -> str:
        return text.replace("```json", "").replace("```", "").strip()

    def _validate_sql(self, sql: str, dialect: str = None) -> tuple[bool, Optional[str]]:
        print(f"  [INFO] Performing static SQL syntax check (Dialect: {dialect or 'auto'})...")
        try:
            sqlglot.parse(sql, read=dialect)
            return True, None
        except Exception as e:
            return False, str(e)

    def _call_ai_with_retry(self, system_instruction, prompt, dialect=None, max_retries=3, dynamic_check=True):
        current_prompt = prompt
        for attempt in range(max_retries):
            raw_response = self._call_ai(system_instruction, current_prompt, is_json=False)
            cleaned_sql = self._clean_sql_code(raw_response)
            
            # 第一重校验：静态语法校验（sqlglot）
            is_valid_static, static_error_msg = self._validate_sql(cleaned_sql, dialect=dialect)
            
            error_msg = None
            if not is_valid_static:
                error_msg = f"Static Syntax Error: {static_error_msg}"
            elif dynamic_check:
                # 第二重校验：动态真机校验（ClickHouse EXPLAIN SYNTAX），拦截臆造字段
                is_valid_dynamic, dynamic_error_msg = self.executor.validate_sql_with_clickhouse(cleaned_sql)
                if not is_valid_dynamic:
                    error_msg = f"ClickHouse Execution Error: {dynamic_error_msg}"
            
            if not error_msg:
                return cleaned_sql
                
            print(f"[WARNING] [Attempt {attempt+1}/{max_retries}] Validation failed: {error_msg}")
            current_prompt = f"{prompt}\n\n---\nPrevious SQL attempt failed. \n**Error details:**\n{error_msg}\n\n**Please self-reflect and correct the SQL, especially check if you hallucinated any fields that are NOT in the exact source metadata!!!**\nIncorrect SQL:\n```sql\n{cleaned_sql}\n```"
            
        raise RuntimeError(f"[ERROR] SQL generation failed after {max_retries} attempts.")

    def _meta_to_markdown_ddl(self, meta_dict: Dict[str, Any]) -> str:
        """从源头上减少 LLM 对复杂 JSON 结构的注意力分散问题，把传入的表元数据由 JSON 转化为 Markdown 格式"""
        if not meta_dict:
            return "No metadata provided."
            
        md_lines = []
        for table_name, meta in meta_dict.items():
            layer = meta.get("layer", "UNKNOWN")
            logic = meta.get("logic_summary", "N/A")
            md_lines.append(f"### 表名: `{table_name}` (所在层级: {layer})")
            md_lines.append(f"**业务逻辑**: {logic}")
            
            columns = meta.get("columns", [])
            if columns:
                md_lines.append("| 字段名 | 数据类型 | 业务注释 |")
                md_lines.append("|---|---|---|")
                for col in columns:
                    cname = col.get("name", "")
                    ctype = col.get("type", "String")
                    ccomment = col.get("comment", "")
                    md_lines.append(f"| {cname} | {ctype} | {ccomment} |")
            md_lines.append("\n")
            
        return "\n".join(md_lines)

    def embed_text(self, text: str) -> List[float]:
        try:
            if self.provider == "aliyun":
                response = self.client.embeddings.create(model="text-embedding-v1", input=text)
                return response.data[0].embedding
            elif self.provider == "google":
                response = self.client.models.embed_content(model="models/text-embedding-004", content=text)
                return response.embeddings[0].values
            else:
                raise ValueError(f"Provider {self.provider} does not support embeddings.")
        except Exception as e:
            print(f"[ERROR] Embedding generation failed: {e}")
            raise

    def update_metadata_from_sql(self, table_name: str, sql_code: str, layer: str) -> Dict[str, Any]:
        system = f"Analyze the {layer} SQL to extract metadata. Output JSON for {table_name}, including layer, base_table, columns (name, type, comment), joins (table, on, type), and logic_summary."
        prompt = f"Table: {table_name}\nSQL:\n{sql_code}"
        response = self._call_ai(system, prompt, is_json=True)
        return json.loads(self._clean_json_code(response)) if isinstance(response, str) else response

    def extract_table_name(self, sql: str) -> Optional[str]:
        patterns = [r"INSERT\s+OVERWRITE\s+TABLE\s+([^\s(`]+)", r"CREATE\s+TABLE\s+.*?\s+([^\s(`]+)"]
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match and match.group(1).upper() not in ["IF", "NOT", "EXISTS"]:
                return match.group(1).strip().replace("`", "")
        return None

    def extract_search_keywords(self, query: str) -> List[str]:
        system = "Extract core business entities from the user query. E.g., '订单头和订单行' -> ['订单头', '订单行']. Output a JSON list of strings."
        try:
            response = self._call_ai(system, query)
            match = re.search(r'\[.*\]', response, re.DOTALL)
            return json.loads(match.group()) if match else [query]
        except:
            return [query]

    def analyze_requirement(self, query: str, relevant_source_meta: Dict[str, Any] = None, relevant_ods_meta: Dict[str, Any] = None, relevant_meta: Dict[str, Any] = None):
        source_tables_info = ""
        if relevant_source_meta:
            descs = []
            for name, meta in relevant_source_meta.items():
                group_cn, group_en = (meta.get("group", "默认组-default") + "-default").split("-", 1)[:2]
                cols = [f"{c.get('name')}({c.get('comment')})" for c in meta.get("columns", [])[:10]]
                descs.append(f"- {name} [Group: {group_cn} | ID: {group_en}]: {meta.get('logic_summary', 'N/A')}. Fields: {', '.join(cols)}")
            source_tables_info = "\n".join(descs)

        ods_tables_info = ""
        if relevant_ods_meta:
            descs = []
            for name, meta in relevant_ods_meta.items():
                group_cn, group_en = (meta.get("group", "默认组-default") + "-default").split("-", 1)[:2]
                cols = [f"{c.get('name')}({c.get('comment')})" for c in meta.get("columns", [])[:10]]
                descs.append(f"- {name} [Group: {group_cn} | ID: {group_en}]: {meta.get('logic_summary', 'N/A')}. Fields: {', '.join(cols)}")
            ods_tables_info = "\n".join(descs)

        existing_tables_info = ""
        if relevant_meta:
            descs = []
            for name, meta in relevant_meta.items():
                if name.startswith('ods_') or name.startswith('src_'): continue
                group_cn, group_en = (meta.get("group", "默认组-default") + "-default").split("-", 1)[:2]
                cols = [f"{c.get('name')}({c.get('comment')})" for c in meta.get("columns", [])[:10]]
                descs.append(f"- {name} [Layer: {meta.get('layer', 'N/A')} | Group: {group_cn} | ID: {group_en}]: {meta.get('logic_summary', 'N/A')}. Fields: {', '.join(cols)}")
            existing_tables_info = "\n".join(descs)
        
        system = f"""
        你是一个数据架构师。请使用指标溯源、分层约束和动态路由来分析此需求。
        动态路由规则：
        1. 涉及明细和报表使用 'dws_' 前缀。
        2. 涉及汇总和指标使用 'ads_' 前缀。

        命名规范规则：
        - ODS: `ods_{{业务源系统库名}}_{{源表名}}`
        - DWD: `dwd_{{主题域}}_{{业务过程}}_{{事实表类型标识}}_{{全量df/增量di标识}}`
        - DIM: `dim_{{主题域/业务实体}}_{{全量df/增量di标识}}`
        - DWS: `dws_{{主题域}}_{{分析实体粒度}}_{{全量df/增量di标识}}`
        - ADS: `ads_{{业务应用场景}}_{{报表/结果集名称}}_{{全量df/增量di标识}}`
        - TMP: `tmp_{{业务过程}}_{{结果描述}}_{{年月日}}`

        **核心命名规范**：所有生成的 DWD、DIM、DWS 和 ADS 表名必须以 `_df` (全量快照) 或 `_di` (增量数据) 结尾，绝对禁止遗漏此后缀。但 ODS 表严禁包含这些后缀。

        架构约束规则：
        1. ADS/DWS 必须读取 DWD 或 DIM 层。
        2. DWD 必须读取 ODS 层。
        3. DWS/ADS 严禁直接读取 ODS 层。如果 DWD 缺少字段，请计划修改并更新 DWD 模型。

        ODS 层生成规则：
        1. 如果需求需要用到 `可用的业务源表` 中的数据，且它尚未在 `已存在的数仓 ODS 表` 中映射，你必须将其输出至 `ODS` 层计划中，并将状态设为 `NEW`（执行动作：'生成 DataX 和 DDL'）。
        2. 如果需要的数据已经在 `已存在的数仓 ODS 表` 中，你必须将其输出，并将状态设为 `EXISTING`（执行动作：'可供 DWD 建模'）。

        业务单元隔离规则（极为重要）：
        1. 严禁将属于不同业务单元的 ODS 表直接 Join（请参考上下文中的 `group` 字段）。
        2. 如果所需的 ODS 表属于不同的 `group`（例如 '订单业务单元-po' vs '发货业务单元-ship'），你必须为每个 group 规划独立的 DWD 表！跨域 Join 只允许发生在 DWS/ADS 层。
        3. 聚合强制规则：对于属于【同一个】业务单元（同一 `group`）的所有 ODS 表，你必须将它们全部整合成【仅仅唯一的一张】DWD 宽表。绝对不允许为同一个业务单元拆分生成多张 DWD 表！

        输出格式：
        请输出包含 `layer_plan` 对象的 JSON，其格式参照：
        {{"ODS": [{{"table_name": "ods_...", "status": "EXISTING/NEW", "source_tables": [], "action_detail": "详细说明：复用或新建"}}], "DWD": [{{"table_name": "dwd_..._df", "status": "EXISTING/NEW", "source_tables": ["ods_..."], "action_detail": "详细说明"}}], "ADS/DWS": [{{"table_name": "dws_..._df", "status": "EXISTING/NEW", "source_tables": ["dwd_..._df"], "action_detail": "详细说明"}}]}}

        **强制要求**：如果任何 DWD 或 ADS/DWS 模型在其 `source_tables` 中引用了上游表，则这些上游表**必须**也出现在对应层级的 `layer_plan` 列表中，说明其组成和来源关系。

        --- 元数据上下文 ---
        可用的业务源表 (Available Source DB Tables):
        {source_tables_info}

        已存在的数仓 ODS 表 (Existing Data Warehouse ODS Tables):
        {ods_tables_info}

        已存在的 DWD/DWS/ADS 表 (Existing DWD/DWS/ADS Tables):
        {existing_tables_info}
        """

        try:
            result = self._call_ai(system, query, response_schema=RequirementAnalysis)
            return result.model_dump()
        except Exception as e:
            print(f"[ERROR] Requirement analysis failed: {e}")
            raise

    def generate_ods(self, meta):
        system = f"""
        你是一个数据集成专家。请严格参考 'Good Smell' 规范模板，为给定的元数据生成 DataX JSON 配置。
        参考模板：
        ```json
        {DATAX_TEMPLATE}
        ```
        生成规则：
        1. 必须根据提供的表结构替换 `source_columns`, `source_table`, `target_columns`, `target_table` 这四个占位符（将其替换为真实的 JSON 数组或字符串）。
        2. 全局强制规则：除上述四个占位符外，所有的 `${{...}}` 参数（如 `${{reader_name}}`, `${{reader_database_username}}` 等）必须**原封不动**保留在生成的 JSON 中！绝对不要尝试把它们替换为任何具体的值。它们将在调度系统中被动态提取解析。
        3. 请仅输出纯净的 JSON 代码，不要包含额外的解释。
        4. 尽量保证 JSON 的 key 顺序和缩进正确，不包含任何特殊字符截断。
        """
        response = self._call_ai(system, f"Metadata for DataX: {json.dumps(meta)}")
        return self._clean_json_code(response)

    def generate_ods_ddl(self, meta, database_name):
        system = f"""
        你是一个数据库架构专家。请严格参考 'Good Smell' 规范模板，为提供的 ODS 元数据生成 ClickHouse 建表 DDL 语句。

        参考模板：
        ```sql
        {CLICKHOUSE_DDL_TEMPLATE}
        ```

        模板动态参数说明：
        - `[database]`：强制替换为 `{database_name}`。
        - `[table_name]`：数据表名称。
        - `[cluster_name]`：ClickHouse 集群名称（如果没有特别指定，统一使用 'sunyur_cluster'）。
        - `[columns]`：字段定义与数据类型映射。
        - `[partition_key]`：分区键（对于 ODS 一般可忽略，如果没有写空）。
        - `[order_key]`：排序键, 一般为来源表的PRIMARY KEY。
        - `[properties]`：其他表属性设置（不要随意添加 TTL，除非有特殊要求。如需性能可以添加：SETTINGS index_granularity = 8192）。
        - `[comment]`：表级别的业务含义中文注释。
        - `[timestamp]`：ReplicatedMergeTree 的 zookeeper 路径时间戳，必须精确到秒（例如：yyyyMMddHHmmss 或 Epoch 秒数），确保每次生成的路径绝对唯一。

        数据类型映射强约束（MySQL 到 ClickHouse）：
        - `INT` 类型 -> `Int32`
        - `BIGINT`, `TINYINT`, `SMALLINT` 等 -> `Int64`
        - `DECIMAL`, `FLOAT`, `DOUBLE` 等 -> `Decimal(30,10)`
        - `VARCHAR`, `CHAR`, `JSON`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` 等字符串类型 -> `String`
        - `DATETIME`, `TIMESTAMP` 类型 -> `DateTime`

        生成规则：
        1. 必须使用参考模板中规定的 Distributed 和 ReplicatedMergeTree 引擎分离结构。
        2. 动态替换模板中所有以 `[]` 标识的变量。
        3. 请仅输出 SQL 代码，不带多余解释。
        """
        return self._call_ai_with_retry(system, f"Metadata: {json.dumps(meta)}", dialect="clickhouse", dynamic_check=False)

    def generate_dwd(self, dwd_meta, ods_meta, production_style, feedback: str = None, pinned_table_name: str = None, source_tables: List[str] = None):
        feedback_prompt = f"User feedback to address: {feedback}" if feedback else ""
        source_instruction = f"MANDATORY: You must only use the following ODS tables as data sources: {', '.join(source_tables)}." if source_tables else ""

        # 从 ods_meta（以 source_db 字段为准）动态提取字段白名单，注入提示词以消除幻觉
        field_whitelist_lines = []
        for tbl_name, tbl_meta in ods_meta.items():
            columns = tbl_meta.get("columns", [])
            if columns:
                col_names = ", ".join(c.get("name", "") for c in columns if c.get("name"))
                field_whitelist_lines.append(f"  - `{tbl_name}`: {col_names}")
        field_whitelist_prompt = ""
        if field_whitelist_lines:
            field_whitelist_prompt = (
                "\n\n**【字段白名单约束 - 最高优先级】**\n"
                "以下是各源表中**真实存在**的字段列表（来自 source_db 原始元数据）。\n"
                "**绝对禁止指令 1**：你必须、且只能使用这里列出的字段！严禁自己臆测、捏造任何未在此列表中出现的字段名（例如：如果列表里只有 `code`，就绝对不能写成 `order_no`）！\n"
                "**绝对禁止指令 2**：在任何子查询或 CTE 中，严禁使用 `SELECT *` 或类似 `SELECT tb.*` 的全选操作！你必须显式地、一个一个地列出所需的业务字段！\n"
                "否则 SQL 将因字段不存在而报错，这是致命错误！\n"
                + "\n".join(field_whitelist_lines)
            )

        system = f"""
        你是一个数仓建模专家。请严格参考 'Good Smell' 规范模板，生成 DWD 层 SQL 代码。
        参考模板：
        ```sql
        {DWD_WIDE_TABLE_TEMPLATE}
        ```

        生成规则：
        1. 数据来源必须、且只能来自所提供的 ODS 元数据。
        2. 必须采用 CTE（公共表表达式）嵌套子查询风格进行编写。
        3. 正确处理各种表连接 (Joins)、枚举类型 (Enums) 以及数据过滤 (Filters)。
        4. 数据清洗规则：必须处理 NULL 值（例如使用 COALESCE 函数赋默认值或 0），并规范化枚举字段的取值。
        5. 防御性编程规则：涉及金额的计算必须使用 COALESCE(amt, 0)，涉及除法的计算必须使用 NULLIF(divisor, 0) 以防止除零错误。
        6. 公共字段强制抽取规则：SELECT 输出的**前两个字段**有严格顺序要求，必须与目标 DDL 建表字段顺序完全一致：第一个字段必须强制为 `toDate(now()) AS _pt`（分区字段赋默认值，确保 INSERT 列顺序与 DDL 对齐）；第二个字段必须强制为 `pur_id`（租户主键，需从底层 ODS 抽取，ODS 层一般含有 purchaser_id 或者 tenant_id，统一提取并输出以作数据隔离）。**严禁遗漏 `_pt` 字段的显式赋值，否则 INSERT 列顺序将与 DDL 错位！**
        7. 字段命名规范：**这极其重要！所有 SELECT 返回的字段名必须全部使用规范的纯英文蛇形命名法 (snake_case)，绝对严禁在 SQL 中使用中文作为字段列名或别名！所有中文描述只能应用于字段的注释 (COMMENT) 阶段。**
        8. 分区过滤规则：必须带上分区过滤条件（例如 `pt='${{bizdate}}'`），严禁执行全表扫描。
        9. 注释覆盖规则：所有生成的表及其字段的注释，必须达到 100% 的注释覆盖率。
        10. 严禁捏造与全选：坚决遵守【字段白名单约束】，每一层的 SELECT 都必须精确指定真实的字段名，绝不可以写出类似于 `SELECT oi.*` 的代码，也不可凭经验补充不存在的实体字段。
        11. 你最终只能输出一条单一的带有 INSERT OVERWRITE 子句的 SQL。

        {source_instruction}
        {feedback_prompt}
        {field_whitelist_prompt}
        """
        prompt = f"DWD Metadata:\n{self._meta_to_markdown_ddl(dwd_meta)}\n\nODS Metadata:\n{self._meta_to_markdown_ddl(ods_meta)}"
        return self._call_ai_with_retry(system, prompt, dialect="hive")


    def generate_dwd_ddl(self, dwd_sql, database_name):
        system = f"""
        你是一个数据库架构专家。请基于提供的 SQL 查询语句，严格参考 'Good Smell' 规范模板生成对应的 DWD 层 ClickHouse 建表 DDL。

        参考模板：
        ```sql
        {CLICKHOUSE_DDL_TEMPLATE}
        ```

        模板动态参数说明：
        - `[database]`：强制替换为 `{database_name}`。
        - `[table_name]`：数据表名称。
        - `[cluster_name]`：ClickHouse 集群名称（如果没有特别指定，统一使用 'sunyur_cluster'）。
        - `[columns]`：这是极其重要的一环。每个表的前两个字段必须强制排布为 `_pt` (类型 Date，建议：_pt Date DEFAULT toDate(now())) 和 `pur_id` (类型 String，租户标识)。其余后面的所有字段你必须精确地从输入 SQL 语句最后返回的 SELECT 列中推导结构，且**生成的建表字段名必须全部是纯英文格式**，不得使用中文作为字段名，中文业务含义统一体现在 COMMENT 注释中！
        - `[partition_key]`：分区键必须固定为 `_pt`。
        - `[order_key]`：排序键必须固定为 `pur_id`。
        - `[properties]`：其他表属性设置。必须包含 `TTL _pt + toIntervalDay(7)`，以及 `SETTINGS index_granularity = 8192`。
        - `[comment]`：必须保证表和它的所有字段具有 100% 的业务含义中文注释覆盖。
        - `[timestamp]`：ReplicatedMergeTree 的 zookeeper 路径时间戳，必须精确到秒（例如：yyyyMMddHHmmss 或 Epoch 秒数），确保每次生成的路径绝对唯一。

        数据类型映射强约束：
        除业务主键外，生成字段类型必须**统一映射**为以下 5 种标准类型之一：
        - `Int32`
        - `Int64`
        - `Decimal(30,10)`
        - `String`
        - `DateTime`

        生成规则：
        1. 必须使用参考模板中规定的 Distributed 和 ReplicatedMergeTree 引擎分离结构。
        2. 全局强制规则：绝不允许漏掉任何列，DDL 字段必须与查询 SQL 字段完全一致。
        3. 替换模板中所有以 `[]` 标识的动态占位符。
        4. 请仅输出 SQL 代码，不带多余解释。
        """
        prompt = f"DWD Query SQL:\n{dwd_sql}"
        return self._call_ai_with_retry(system, prompt, dialect="clickhouse", dynamic_check=False)

    def generate_ads(self, analysis, dwd_ddl, feedback: str = None, pinned_table_name: str = None, source_tables: List[str] = None):
        req_type = analysis.get("requirement_type", "DETAIL")
        template = DWS_DETAIL_TEMPLATE if req_type == "DETAIL" else ADS_AGGREGATION_TEMPLATE
        style_desc = "DWS Detail Report" if req_type == "DETAIL" else "ADS Aggregation Report"
        feedback_prompt = f"User feedback to address: {feedback}" if feedback else ""
        source_instruction = f"MANDATORY: You must only use the following upstream tables as data sources: {', '.join(source_tables)}." if source_tables else ""

        system = f"""
        你是一个 BI 开发与应用层建模专家。请严格参考 'Good Smell' 规范模板生成 {style_desc} 层的 SQL。
        参考模板：
        ```sql
        {template}
        ```

        生成规则：
        1. 数据来源必须、且只能来自 DWD 层。禁止在这一层包含过于复杂的清洗逻辑或跨层级直接读取 ODS 表。
        2. 针对 DWS 层：此层需要计算各种派生指标，并对数据按维度进行轻度或重度聚合。
        3. 针对 ADS 层：保持表结构极度扁平化（大宽表或 KV 结构），避免复杂的关联和聚合，以保证毫秒级的查询响应。
        4. 数据倾斜防护规则：如果在包含大量 NULL 值的超大表上执行 GROUP BY 或 JOIN，必须引入随机数将其打散重分布。
        5. 公共字段强制抽取规则：SELECT 输出的**前两个字段**有严格顺序要求，必须与目标 DDL 建表字段顺序完全一致：第一个字段必须强制为 `toDate(now()) AS _pt`（分区字段赋默认值，确保 INSERT 列顺序与 DDL 对齐）；第二个字段必须强制为 `pur_id`（租户主键，需从底层抽取以作数据隔离）。**严禁遗漏 `_pt` 字段的显式赋值，否则 INSERT 列顺序将与 DDL 错位！**
        6. 字段命名规范：**这极其重要！所有 SELECT 返回的字段名（包括聚合指标的别名）必须全部使用规范的纯英文蛇形命名法 (snake_case)，绝对严禁在 SQL 层使用中文列名！中文只能应用于日后的字段 COMMENT 中。**
        7. 注释覆盖规则：所有生成的表及其字段必须拥有 100% 的业务含义中文注释。
        8. 你最后只能输出一条带有 INSERT OVERWRITE 的标准 SQL。

        {source_instruction}
        {feedback_prompt}
        """
        prompt = f"Analysis: {json.dumps(analysis)}\nDWD DDL:\n{dwd_ddl}"
        # 这里如果 analysis 里有其他传入的字典型 meta 也可以转换
        return self._call_ai_with_retry(system, prompt, dialect="hive")

    def generate_ads_ddl(self, ads_sql, database_name):
        system = f"""
        你是一个数据库架构专家。请基于提供的 SQL 查询语句，严格参考 'Good Smell' 规范模板生成对应的 ADS/DWS 应用层 ClickHouse 建表 DDL。
        参考模板：
        ```sql
        {CLICKHOUSE_DDL_TEMPLATE}
        ```

        模板动态参数说明：
        - `[database]`：强制替换为 `{database_name}`。
        - `[table_name]`：数据表名称。
        - `[cluster_name]`：ClickHouse 集群名称（如果没有特别指定，统一使用 'sunyur_cluster'）。
        - `[columns]`：这是极其重要的一环。每个表的前两个字段必须强制排布为 `_pt` (类型 Date，建议：_pt Date DEFAULT toDate(now())) 和 `pur_id` (类型 String，租户标识)。其余后面的所有字段你必须精确地从输入 SQL 语句最后返回的 SELECT 列中推导结构，且**生成的建表字段名必须全部是纯英文格式**，不得使用中文作为字段名，中文业务含义统一体现在 COMMENT 注释中！
        - `[partition_key]`：分区键必须固定为 `_pt`。
        - `[order_key]`：排序键必须固定为 `pur_id`。
        - `[properties]`：其他表属性设置。必须包含 `TTL _pt + toIntervalDay(7)`，以及 `SETTINGS index_granularity = 8192`。
        - `[comment]`：表级别和所有字段级别必须拥有 100% 的中文注释。
        - `[timestamp]`：ReplicatedMergeTree 的 zookeeper 路径时间戳，必须精确到秒（例如：yyyyMMddHHmmss 或 Epoch 秒数），确保每次生成的路径绝对唯一。

        数据类型映射强约束：
        除业务主键外，生成字段类型必须**统一映射**为以下 5 种标准类型之一：
        - `Int32`
        - `Int64`
        - `Decimal(30,10)`
        - `String`
        - `DateTime`

        生成规则：
        1. 必须使用参考模板中规定的 Distributed 和 ReplicatedMergeTree 引擎分离结构。
        2. 全局强制约束：表结构字段必须与输入 SQL 语句返回的 SELECT 字段100%匹配。
        3. 请把模板里的所有 `[]` 动态参数替换为有意义的真实设定。
        4. 请仅输出 SQL 代码，不带任何其他解释。
        """
        return self._call_ai_with_retry(system, f"ADS Query SQL:\n{ads_sql}", dialect="clickhouse", dynamic_check=False)
