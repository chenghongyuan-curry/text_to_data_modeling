import os
import json
from typing import Dict, Any # 导入 Dict 和 Any
from .engine import AutoDWEngine
from .knowledge_manager import KnowledgeManager

class AutoDWPipeline:
    def __init__(self):
        """初始化流水线组件"""
        try:
            self.engine = AutoDWEngine()
            self.km = KnowledgeManager()
        except ValueError as e:
            print(f"[ERROR] Initialization failed: {e}")
            raise

    def run_interactive(self, initial_query: str):
        """执行交互式数仓生成流水线 (Human-in-the-Loop)"""
        print(f"\n[START] Starting text-to-report Interactive Pipeline...")
        current_query = initial_query

        # 1. 知识入库 & 检索
        self._step_knowledge_retrieval(current_query)

        # 2. 需求分析与确认循环
        while True:
            analysis = self._step_analyze(current_query)
            print(f"\n[INTERACTION] 需求分析结果: {analysis['requirement_type']} -> 目标表: {analysis['target_table']}")
            
            user_input = input(">> 确认需求分析结果? (y: 继续 / n: 修改需求 / q: 退出): ").strip().lower()
            if user_input == 'y':
                break
            elif user_input == 'q':
                print("[INFO] 用户取消操作。")
                return
            else:
                current_query = input(">> 请输入新的需求描述: ").strip()
                # 重新检索知识库，因为需求变了
                self._step_knowledge_retrieval(current_query)

        # 3. ODS 层生成 (自动执行，通常不需要人工干预)
        self._step_generate_ods(analysis)

        # 4. DWD 层生成与确认循环
        dwd_context = ""
        dwd_feedback = None
        while True:
            # 生成 DWD (传入 analysis)
            dwd_context = self._step_generate_dwd(analysis, feedback=dwd_feedback)
            
            if not dwd_context:
                print("[INFO] DWD 层处理完毕或无变更。")
                break 

            print("\n[INTERACTION] DWD 层代码已生成。请检查 output/dwd/ 目录下的文件。")
            user_input = input(">> 确认 DWD 逻辑? (y: 继续 / n: 提供修改意见 / q: 退出): ").strip().lower()
            
            if user_input == 'y':
                break
            elif user_input == 'q':
                print("[INFO] 用户取消操作。")
                return
            else:
                dwd_feedback = input(">> 请输入 DWD 修改意见 (例如: '增加逻辑删除过滤'): ").strip()
                print("[INFO] 正在根据反馈重新生成 DWD...")

        # 5. 服务层 (DWS/ADS) 生成与确认循环
        service_feedback = None
        while True:
            self._step_generate_service_layer(analysis, dwd_context, feedback=service_feedback)
            
            target_layer = "DWS" if analysis.get("requirement_type") == "DETAIL" else "ADS"
            layer_dir = target_layer.lower()
            print(f"\n[INTERACTION] 服务层 ({target_layer}) 代码已生成。请检查 output/{layer_dir}/ 目录下的文件。")
            user_input = input(">> 确认最终报表逻辑? (y: 完成 / n: 提供修改意见 / q: 退出): ").strip().lower()
            
            if user_input == 'y':
                break
            elif user_input == 'q':
                print("[INFO] 用户取消操作。")
                return
            else:
                service_feedback = input(">> 请输入修改意见 (例如: '修改聚合口径'): ").strip()
                print("[INFO] 正在根据反馈重新生成服务层代码...")

        print("\n[SUCCESS] Pipeline execution finished successfully!")

    def run(self, user_query: str):
        """执行完整的数仓生成流水线 (非交互模式，保留兼容性)"""
        print(f"\n[START] Starting text-to-report Pipeline...")
        print(f"[INFO] Original Requirement: {user_query}")

        # 1. 知识入库 & 检索 (RAG)
        self._step_knowledge_retrieval(user_query)
        
        # 2. 需求分析
        analysis = self._step_analyze(user_query)
        
        # 3. ODS 层生成
        self._step_generate_ods(analysis)
        
        # 4. DWD 层生成
        dwd_context = self._step_generate_dwd(analysis)
        
        # 5. DWS/ADS 服务层生成
        self._step_generate_service_layer(analysis, dwd_context)
        
        print("\n[SUCCESS] Pipeline execution finished. Check outputs in the output directory.")

    def _step_knowledge_retrieval(self, query):
        """步骤1: 向量知识库检索 (多路循环) + 邻居发现"""
        # 强制清理旧数据，确保元数据实时同步
        self.km.reset_collection()
        
        print(f"[INFO] Building/Updating metadata vector index (RAG) from directory...")
        self.km.ingest_metadata('metadata/source_db')
        if os.path.exists('metadata/ods'):
            self.km.ingest_metadata('metadata/ods')
        
        # 1. 实体拆分：将需求拆解为独立实体 (如: 订单头, 订单行)
        entities = self.engine.extract_search_keywords(query)
        print(f"  [RAG] Extracted Entities: {entities}")
        
        all_relevant_meta = {}
        
        # 2. 多路并发检索 (针对每个实体搜一次)
        for entity in entities:
            print(f"  [RAG] Searching for entity: {entity}...")
            entity_meta = self.km.search_related_tables(entity, top_k=8)
            all_relevant_meta.update(entity_meta)
        
        # 3. 邻居发现 (Sibling Discovery)
        # 针对 source_db 寻找邻居，确保同一个库的表能被一起拉出来
        sibling_meta = {}
        for table_name in all_relevant_meta.keys():
            if table_name.startswith('ods_') or table_name.startswith('src_'):
                parts = table_name.split('_')
                if len(parts) >= 3:
                    db_prefix = "_".join(parts[:4]) 
                    # 分别在 source_db 和 ods 里找同源的表
                    neighbors_src = self._find_tables_by_prefix(db_prefix, 'metadata/source_db')
                    neighbors_ods = self._find_tables_by_prefix(db_prefix, 'metadata/ods')
                    
                    for n_name, n_meta in neighbors_src.items():
                        if n_name not in all_relevant_meta:
                            sibling_meta[n_name] = n_meta
                    for n_name, n_meta in neighbors_ods.items():
                        if n_name not in all_relevant_meta:
                            sibling_meta[n_name] = n_meta
        
        all_relevant_meta.update(sibling_meta)
        
        # 4. 血缘增强 (Lineage Enhancement)
        # 原有逻辑：由 DWD 找其源表 ODS
        enhanced_meta = all_relevant_meta.copy()
        for table_name, meta in all_relevant_meta.items():
            if meta.get('layer') in ['DWD', 'DWM', 'DWS', 'ADS']:
                base_table = meta.get('base_table')
                if base_table and base_table not in enhanced_meta:
                    print(f"  [LINEAGE] Auto-loading source table: {base_table}")
                    ods_meta = self._load_single_metadata(base_table, 'metadata/ods')
                    if ods_meta:
                        enhanced_meta.update(ods_meta)
        
        relevant_meta = enhanced_meta
        
        # 新增物理隔离加载
        self.source_meta = self._load_metadata_from_dir('metadata/source_db')
        self.ods_meta = self._load_metadata_from_dir('metadata/ods')
        
        # 过滤 RAG 命中结果，将其严格归类
        self.relevant_source_meta = {k: v for k, v in relevant_meta.items() if k in self.source_meta}
        self.relevant_ods_meta = {k: v for k, v in relevant_meta.items() if k in self.ods_meta}
        
        self.dwd_meta = {k: v for k, v in relevant_meta.items() if k.startswith('dwd_') or k.startswith('dwm_')}
        self.relevant_meta_full = relevant_meta 
        
        print(f"[INFO] RAG Retrieval Hit (Enhanced): {len(relevant_meta)} tables (Source: {len(self.relevant_source_meta)}, ODS_Exist: {len(self.relevant_ods_meta)}, DWD: {len(self.dwd_meta)})")

    def _find_tables_by_prefix(self, prefix, directory):
        """根据前缀寻找邻居表"""
        results = {}
        if not os.path.exists(directory):
            return results
        for root, _, files in os.walk(directory):
            for file in files:
                if file.startswith(prefix) and file.endswith('.json'):
                    with open(os.path.join(root, file), 'r') as f:
                        meta = json.load(f)
                        results.update(meta)
        return results

    def _step_analyze(self, query):
        """步骤2: 利用 LLM 分析需求，并对齐现有表名"""
        # 将 RAG 命中的两份物理隔离的元数据作为上下文传入
        analysis = self.engine.analyze_requirement(query, self.relevant_source_meta, self.relevant_ods_meta, self.relevant_meta_full)
        
        
        # 【重要后处理】强制验证 AI 对 EXISTING 表的判断 (防止幻觉)
        self._post_process_analysis_plan(analysis)

        status_msg = "Reusing existing table" if not analysis['is_new_table'] else "Creating new table"
        print(f"\n[STEP 1/4] Requirement analysis completed: {status_msg} -> {analysis['target_table']}")
        
        # 打印层级计划
        print("  [LAYER PLAN] 待处理表及执行计划:")
        layer_plan = analysis.get('layer_plan', {})
        for layer, tables in layer_plan.items():
            valid_tables = []
            for item in tables:
                if isinstance(item, dict):
                    table_name = item.get('table_name') or item.get('table')
                    status = item.get('status', 'UNKNOWN')
                    action = item.get('action_detail', '')
                    action_str = f" \n        ↳ 执行动作: {action}" if action else ""
                    if table_name:
                        valid_tables.append(f"表名: {table_name} (状态: {status}){action_str}")
                else:
                    valid_tables.append(str(item))

            tables_str = "\n      - ".join([t for t in valid_tables if t and str(t).lower() not in ['none', 'n/a', 'null']]) if valid_tables else "无需处理或元数据缺失"
            print(f"    - {layer}:\n      - {tables_str}" if valid_tables and tables_str != "无需处理或元数据缺失" else f"    - {layer}: {tables_str}")
            
        return analysis

    def _post_process_analysis_plan(self, analysis: Dict[str, Any]):
        """
        后处理分析计划，根据语义匹配和 output 层级文件是否存在，判断表状态为 NEW 还是 EXISTING。
        如果在 output 的对应层级中未找到，则为 NEW；
        如果根据语义匹配到（且由于没有报错，代表有文件或符合业务）则为 EXISTING。
        """
        layer_plan = analysis.get('layer_plan', {})
        
        def get_output_path(layer, t_name):
            if layer == 'ODS':
                return f"output/ods/{t_name}.json"
            elif layer == 'DWD':
                return f"output/dwd/{t_name}.sql"
            elif layer == 'ADS/DWS':
                layer_dir = "dws" if t_name.startswith("dws_") else "ads"
                return f"output/{layer_dir}/{t_name}.sql"
            return ""

        for layer in ['ODS', 'DWD', 'ADS/DWS']:
            for i, entry in enumerate(layer_plan.get(layer, [])):
                if isinstance(entry, dict):
                    table_name = entry.get('table_name') or entry.get('table')
                    if not table_name:
                        continue
                        
                    # 判断是否存在输出文件
                    out_path = get_output_path(layer, table_name)
                    has_output = os.path.exists(out_path)
                    
                    # 判断是否在语义元数据中
                    in_meta = table_name in self.relevant_meta_full
                    
                    original_status = str(entry.get('status', '')).upper()
                    
                    if layer == 'ODS':
                        # ODS 层主要依据元数据判断是否存在，而不是依据 output 文件是否被清理
                        if in_meta and original_status != 'NEW':
                            entry['status'] = 'EXISTING'
                        elif not in_meta:
                            entry['status'] = 'NEW'
                            print(f"[INFO] 调整表状态: {table_name} (ODS) -> NEW (元数据中未找到)")
                    else:
                        if not has_output:
                            # DWD/DWS 层：在 output 中未找到，则为 new
                            entry['status'] = 'NEW'
                            if original_status != 'NEW':
                                print(f"[INFO] 调整表状态: {table_name} ({layer}) -> NEW (对应输出文件未找到)")
                        else:
                            if in_meta:
                                # 根据语义匹配到，且存在产出物，则为已存在
                                entry['status'] = 'EXISTING'
                            else:
                                # 如果既没语义匹配，文件又存在，通常当成 NEW 以便更新覆盖
                                entry['status'] = 'NEW'
                                print(f"[INFO] 调整表状态: {table_name} ({layer}) -> NEW (虽有文件但不匹配语义)")

        analysis['layer_plan'] = layer_plan # 更新 analysis 对象

        # 确保 target_table 以及 overall 的 is_new_table 状态保持一致
        target_table_name = analysis.get('target_table')
        if layer_plan:
            analysis['is_new_table'] = False
            for layer in ['ADS/DWS', 'DWD', 'ODS']:
                for entry in layer_plan.get(layer, []):
                    if isinstance(entry, dict) and entry.get('status') == 'NEW':
                        analysis['is_new_table'] = True
                        break
                if analysis['is_new_table']:
                    break


    def _step_generate_ods(self, analysis):
        """步骤3: 根据分析计划生成 ODS 层代码 (DataX)"""
        print("\n[STEP 2/4] Developing ODS Layer (DataX)...")
        
        layer_plan = analysis.get('layer_plan', {})
        planned_ods_tables_raw = layer_plan.get('ODS', [])
        
        if not planned_ods_tables_raw:
            print("[INFO] No specific ODS tables found in LAYER PLAN. Skipping ODS generation.")
            return

        # 构建需要处理的 ODS 表名清单 (去除可能的状态信息)
        planned_ods_table_names = []
        for ods_entry in planned_ods_tables_raw:
            is_new = True # 默认认为是新表
            if isinstance(ods_entry, dict):
                table_name = ods_entry.get('table_name') or ods_entry.get('table')
                if ods_entry.get('status') == 'EXISTING':
                    is_new = False
            else:
                table_name = str(ods_entry).split(' ')[0].strip()
                if "EXISTING" in str(ods_entry).upper():
                    is_new = False
                    
            if table_name and is_new:
                planned_ods_table_names.append(table_name)
            elif table_name and not is_new:
                print(f"  [INFO] Skipping ODS table '{table_name}' because its status is 'EXISTING'.")
        
        if not planned_ods_table_names:
            print("[INFO] No valid ODS table names extracted from LAYER PLAN. Skipping ODS generation.")
            return

        print(f"[INFO] ODS tables to process from LAYER PLAN: {', '.join(planned_ods_table_names)}")

        processed_count = 0
        for table_name in planned_ods_table_names:
            print(f"  [INFO] Processing table: {table_name}...")
            # 因为是生成 ODS，我们需要其源表结构，因此从 source_meta 里查找
            meta_for_generation = self.source_meta.get(table_name)
            
            # 如果 RAG 没命中，则从磁盘加载
            if not meta_for_generation:
                loaded_meta = self._load_single_metadata(table_name, 'metadata/ods')
                if loaded_meta and table_name in loaded_meta:
                    meta_for_generation = loaded_meta[table_name]
            
            if not meta_for_generation:
                print(f"  [WARNING] Metadata for planned ODS table '{table_name}' not found (neither in RAG hits nor on disk). Skipping generation for this table.")
                continue

            single_table_meta = {table_name: meta_for_generation}
            
            ods_code = self.engine.generate_ods(single_table_meta)
            ods_ddl = self.engine.generate_ods_ddl(single_table_meta)
            
            self._save_file(f'output/ods/{table_name}.json', ods_code)
            self._save_file(f'output/ods/{table_name}.ddl', ods_ddl)
            print(f"     [SUCCESS] Generated: output/ods/{table_name}.json & .ddl")
            processed_count += 1
        
        if processed_count == 0:
            print("[WARNING] No ODS tables were successfully processed based on the LAYER PLAN.")
    def _step_generate_dwd(self, analysis, feedback: str = None):
        """步骤3: 基于分析计划生成 DWD 层代码 (模型驱动)"""
        print("\n[STEP 3/4] Developing DWD Layer (SQL)...")
        
        layer_plan = analysis.get('layer_plan', {})
        dwd_tables = layer_plan.get('DWD', [])
        
        if not dwd_tables:
            print("[INFO] No DWD tables planned, skipping.")
            return ""

        production_style = "Hive/Clickhouse Standard, PARTITIONED BY _pt, 嵌套子查询"
        dwd_ddl_context = "" 
        
        for dwd_entry in dwd_tables:
            # 兼容处理: 可能是字符串 "table_name (STATUS)"，也可能是字典 {'table_name': '...'}
            source_tables = []
            if isinstance(dwd_entry, dict):
                table_name = dwd_entry.get('table_name') or dwd_entry.get('table')
                source_tables = dwd_entry.get('source_tables', [])
            else:
                table_name = str(dwd_entry).split(' ')[0].strip()

            if not table_name: continue
            
            is_new = "NEW" in str(dwd_entry).upper()
            
            print(f"  [INFO] Processing DWD table: {table_name} (Status: {'NEW' if is_new else 'EXISTING'}, Source: {source_tables})...")
            
            # 准备目标元数据
            if is_new:
                target_meta = {table_name: {
                    "layer": "DWD", 
                    "group": analysis.get("group_en", "default"),
                    "logic_summary": analysis.get("logic_summary", "新模型需求")
                }}
            else:
                target_meta = {k: v for k, v in self.dwd_meta.items() if k == table_name}
                if not target_meta:
                    target_meta = self._load_single_metadata(table_name, 'metadata/dwd') or {}
            
            # 从 DWD 表元数据中解析出其业务组
            dwd_group_en = (target_meta.get(table_name, {}).get("group") or "default")

            # 以 source_meta（source_db 原始字段）为准构建 ODS 字段上下文，确保字段白名单准确
            # 按业务组过滤，实现物理隔离
            scoped_ods_meta = {
                name: meta
                for name, meta in self.source_meta.items()
                if (meta.get("group", "default-").split('-')[-1] or "default") == dwd_group_en
            }
            # 若 source_tables 计划表名已知，则精准过滤只保留计划中用到的表
            if source_tables:
                filtered = {k: v for k, v in scoped_ods_meta.items() if k in source_tables}
                if filtered:
                    scoped_ods_meta = filtered
            print(f"  [INFO] ODS 字段上下文（来源: source_db），group='{dwd_group_en}'，共 {len(scoped_ods_meta)} 张表: {list(scoped_ods_meta.keys())}")

            dwd_code = self.engine.generate_dwd(
                target_meta,
                scoped_ods_meta,  # 以 source_db 字段为准的上下文
                production_style,
                feedback=feedback,
                pinned_table_name=table_name,
                source_tables=source_tables
            )
            
            # 使用生成的 SQL 来逆向推导 DDL，保证字段 100% 对应
            print(f"  [INFO] Generating DDL for {table_name} based on generated SQL...")
            dwd_ddl = self.engine.generate_dwd_ddl(dwd_code)
            dwd_ddl_context += dwd_ddl + "\n"
            
            self._save_file(f'output/dwd/{table_name}.sql', dwd_code)
            self._save_file(f'output/dwd/{table_name}.ddl', dwd_ddl)
            
            print(f"  [INFO] Syncing metadata for {table_name}...")
            updated_meta = self.engine.update_metadata_from_sql(table_name, dwd_code, "DWD")
            self._save_file(f'metadata/dwd/{table_name}.json', json.dumps(updated_meta, indent=4, ensure_ascii=False))
            print(f"     [SUCCESS] Generated: output/dwd/{table_name}.sql & Sync'd metadata/dwd/{table_name}.json")
            
        return dwd_ddl_context

    def _step_generate_service_layer(self, analysis, dwd_context, feedback: str = None):
        """步骤4: 基于分析计划生成 DWS/ADS 层代码 (服务层)"""
        print("\n[STEP 4/4] Developing Service Layer (DWS/ADS)...")
        
        layer_plan = analysis.get('layer_plan', {})
        service_tables = layer_plan.get('ADS/DWS', [])
        
        if not service_tables:
            service_tables = [analysis.get('target_table')]

        for service_entry in service_tables:
            if not service_entry: continue
            
            source_tables = []
            if isinstance(service_entry, dict):
                table_name = service_entry.get('table_name') or service_entry.get('table')
                source_tables = service_entry.get('source_tables', [])
            else:
                table_name = str(service_entry).split(' ')[0].strip()
            
            if not table_name: continue

            print(f"  [INFO] Generating Service table: {table_name} (Source: {source_tables})...")
            
            sql_code = self.engine.generate_ads(analysis, dwd_context, feedback=feedback, pinned_table_name=table_name, source_tables=source_tables)
            layer_dir = "dws" if table_name.startswith("dws_") else "ads"
            target_layer = layer_dir.upper()

            print(f"  [INFO] Generating DDL for {table_name}...")
            ddl_code = self.engine.generate_ads_ddl(sql_code)
            
            self._save_file(f'output/{layer_dir}/{table_name}.sql', sql_code)
            self._save_file(f'output/{layer_dir}/{table_name}.ddl', ddl_code)
            
            print(f"  [INFO] Syncing metadata for {table_name}...")
            updated_meta = self.engine.update_metadata_from_sql(table_name, sql_code, target_layer)
            self._save_file(f'metadata/{layer_dir}/{table_name}.json', json.dumps(updated_meta, indent=4, ensure_ascii=False))
            print(f"     [SUCCESS] Generated: output/{layer_dir}/{table_name}.sql & Sync'd metadata/{layer_dir}/{table_name}.json")

    def _load_single_metadata(self, table_name, directory):
        """从目录中加载单个表的 JSON"""
        if not os.path.exists(directory):
            return None
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.json'):
                    with open(os.path.join(root, file), 'r') as f:
                        meta = json.load(f)
                        if table_name in meta:
                            return {table_name: meta[table_name]}
        return None

    def _load_metadata_from_dir(self, directory):
        """辅助方法：从目录加载所有 JSON 元数据"""
        combined = {}
        if not os.path.exists(directory): return combined
        for file in os.listdir(directory):
            if file.endswith('.json'):
                with open(os.path.join(directory, file), 'r') as f:
                    combined.update(json.load(f))
        return combined

    def _save_file(self, path, content):
        """辅助方法：保存文件并自动创建目录"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(content)