import json
import os

def parse_port_string(port_string):
    """
    解析类似 'fulfillment_db-shipments_item-id' 的字符串，
    返回 (数据库名, 表名, 字段名)。
    """
    parts = port_string.split('-')
    if len(parts) >= 3:
        db_name = parts[0]
        table_name = '-'.join(parts[1:-1]) # 处理表名中可能包含'-'的情况
        field_name = parts[-1]
        return db_name, table_name, field_name
    return None, None, None

def transform_metadata(source_file="/Users/chenghongyuan/PycharmProjects/MyFirstPython/text_to_data_modeling/utils/business_metadata.json", output_dir="metadata/source_db"):
    """
    将业务导出的 ER 图 JSON 转换为项目所需的 ODS 元数据格式，并基于 port 字符串精确解析关联关系。
    """
    print(f"Starting transformation of '{source_file}'...")

    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            business_data = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] Source file not found: '{source_file}'.")
        return
    except json.JSONDecodeError:
        print(f"[ERROR] Invalid JSON format in '{source_file}'.")
        return

    uml_content = business_data.get("data", {}).get("umlContent", {})
    source_db_name = business_data.get("data", {}).get("dbName")
    tables = uml_content.get("tables", [])
    edges = uml_content.get("edges", [])

    if not source_db_name or not tables:
        print("[ERROR] Could not find 'dbName' or 'tables' in the source JSON.")
        return

    table_map = {table['tableName']: table for table in tables}
    relations_map = {table_name: [] for table_name in table_map.keys()}

    for edge in edges:
        source_port = edge.get("source", {}).get("port")
        target_port = edge.get("target", {}).get("port")
        cardinality_text = edge.get("label", {}).get("text", "1:n").replace(" ", "") # 默认为 1:n

        if not source_port or not target_port:
            continue

        s_db, s_table, s_field = parse_port_string(source_port)
        t_db, t_table, t_field = parse_port_string(target_port)

        if not all([s_db, s_table, s_field, t_db, t_table, t_field]):
            continue

        ods_source_table_name = f"ods_{s_db}_{s_table}"
        ods_target_table_name = f"ods_{t_db}_{t_table}"

        # 为源表添加目标关系
        if s_table in relations_map:
            relations_map[s_table].append({
                "target": ods_target_table_name,
                "on": f"{ods_source_table_name}.{s_field} = {ods_target_table_name}.{t_field}",
                "cardinality": cardinality_text
            })
        
        # 为目标表添加源关系
        if t_table in relations_map:
            # 反转基数
            reversed_cardinality = ":".join(cardinality_text.split(":")[::-1])
            relations_map[t_table].append({
                "target": ods_source_table_name,
                "on": f"{ods_target_table_name}.{t_field} = {ods_source_table_name}.{s_field}",
                "cardinality": reversed_cardinality
            })

    for original_table_name, table_data in table_map.items():
        try:
            ods_table_name = f"ods_{source_db_name}_{original_table_name}"
            
            columns = [{"name": f.get("name"), "type": f.get("type"), "comment": f.get("comment", ""), "nullable": f.get("nullable", True), "primary_key": f.get("primaryKey", False)} for f in table_data.get("fields", [])]

            ods_meta = {
                ods_table_name: {
                    "layer": "ODS",
                    "source_db": source_db_name,
                    "source_table": original_table_name,
                    "logic_summary": table_data.get("comment", ""),
                    "description": table_data.get("comment", ""),
                    "group": "订单业务单元-po",
                    "columns": columns,
                    "relations": relations_map.get(original_table_name, [])
                }
            }

            output_filename = os.path.join(output_dir, f"{ods_table_name}.json")
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(ods_meta, f, indent=4, ensure_ascii=False)
            
            print(f"  [SUCCESS] Transformed and saved: {output_filename}")

        except Exception as e:
            print(f"  [ERROR] Failed to process table '{original_table_name}': {e}")

    print(f"\nTransformation complete. Processed {len(tables)} tables.")

if __name__ == "__main__":
    transform_metadata()
