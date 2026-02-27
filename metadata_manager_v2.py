import json
import re
from typing import Dict, List, Any, Optional
import os


class TextJsonParser:
    """
    解析txt文件中的JSON数据，生成目标JSON格式
    文件命名规则：ods+_数据库名+_表名
    """

    def __init__(self):
        self.parsed_data = {}
        self.tables = []
        self.edges = []
        self.groups = []

    def parse_text_file(self, text_file_path: str) -> List[Dict]:
        """
        解析文本文件中的JSON数据

        Args:
            text_file_path: 文本文件路径

        Returns:
            解析后的元数据列表
        """
        with open(text_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 提取JSON部分
        json_match = re.search(r'(\{.*\})', content, re.DOTALL)
        if not json_match:
            raise ValueError("未找到有效的JSON数据")

        json_str = json_match.group(1)
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")
            # 尝试修复不完整的JSON
            data = self._fix_and_parse_json(json_str)

        # 提取核心数据
        uml_content = data.get('data', {}).get('umlContent', {})
        self.tables = uml_content.get('tables', [])
        self.edges = uml_content.get('edges', [])
        self.groups = uml_content.get('groups', [])

        return self._generate_target_jsons(data.get('data', {}))

    def _fix_and_parse_json(self, json_str: str) -> Dict:
        """
        尝试修复不完整的JSON字符串

        Args:
            json_str: JSON字符串

        Returns:
            解析后的字典
        """
        # 移除可能的不完整部分
        fixed_json = re.sub(r',\s*}', '}', json_str)
        fixed_json = re.sub(r',\s*]', ']', fixed_json)

        # 尝试解析修复后的JSON
        try:
            return json.loads(fixed_json)
        except json.JSONDecodeError:
            # 如果仍然失败，尝试进一步修复
            lines = fixed_json.split('\n')
            cleaned_lines = []
            for line in lines:
                line = line.strip()
                if line.endswith(','):
                    # 检查下一行是否是对象或数组的结束
                    continue
                cleaned_lines.append(line)

            final_json = '\n'.join(cleaned_lines)
            return json.loads(final_json)

    def _generate_target_jsons(self, er_data: Dict) -> List[Dict]:
        """
        根据ER图数据生成目标JSON格式

        Args:
            er_data: ER图数据

        Returns:
            目标JSON格式的列表
        """
        result = []
        db_name = er_data.get('dbName', 'default_db')
        if not db_name:
            db_name = 'fulfillment_db'  # 默认数据库名

        for table in self.tables:
            table_json = self._generate_table_json(table, db_name)
            result.append(table_json)

        return result

    def _generate_table_json(self, table: Dict, db_name: str) -> Dict:
        """
        为单个表生成目标JSON格式

        Args:
            table: 表定义
            db_name: 数据库名称

        Returns:
            目标JSON格式
        """
        # 获取表所属的组信息
        group_info = self._find_group_by_table(table)
        group_title = group_info.get('title', '默认业务单元') if group_info else '默认业务单元'

        # 构建列信息
        columns = self._parse_columns(table.get('fields', []))

        # 构建关系信息
        relations = self._parse_relations(table.get('id'))

        # 构建表名（作为JSON的key）
        table_name = table.get('tableName', 'unknown_table')
        table_comment = table.get('comment', f"{table.get('label', f'{table_name}表')}")

        # 根据表名生成规范的ODS表名
        ods_table_name = self._generate_ods_table_name(db_name, table_name)

        table_json = {
            ods_table_name: {
                "group": group_title,
                "source": {
                    "type": "mysql",
                    "db": db_name,
                    "table": table_name,
                    "comment": table_comment
                },
                "columns": columns,
                "relations": relations
            }
        }

        return table_json

    def _generate_ods_table_name(self, db_name: str, table_name: str) -> str:
        """
        生成ODS规范的表名：ods+_数据库名+_表名

        Args:
            db_name: 数据库名
            table_name: 原始表名

        Returns:
            ODS规范表名
        """
        # 清理数据库名和表名，移除特殊字符
        db_clean = db_name.replace('-', '_').replace('.', '_').lower().strip()
        table_clean = table_name.replace('-', '_').replace('.', '_').lower().strip()

        # 生成ODS表名：ods+_数据库名+_表名
        return f"ods_{db_clean}_{table_clean}"

    def _find_group_by_table(self, table: Dict) -> Optional[Dict]:
        """
        根据表查找所属组信息

        Args:
            table: 表定义

        Returns:
            组信息或None
        """
        parent_id = table.get('parentId')
        for group in self.groups:
            if group.get('id') == parent_id:
                return group
        return None

    def _parse_columns(self, fields: List[Dict]) -> List[Dict]:
        """
        解析字段信息为columns格式

        Args:
            fields: 字段列表

        Returns:
            columns格式列表
        """
        columns = []
        for field in fields:
            # 从id中提取字段类型和表名信息
            field_id = field.get('id', '')
            # id格式通常是: "database-table-field_name"
            parts = field_id.split('-')
            if len(parts) >= 3:
                db_part = parts[0]
                table_part = parts[1]
                field_name = '-'.join(parts[2:])  # 处理字段名中可能包含的连字符

            column = {
                "name": field.get('name'),
                "type": self._map_field_type(field.get('type')),
                "comment": field.get('comment', ''),
            }

            # 如果是主键，则添加is_primary标识
            if field.get('primaryKey'):
                column["is_primary"] = True

            columns.append(column)

        return columns

    def _map_field_type(self, field_type: str) -> str:
        """
        将原始字段类型映射为MySQL类型

        Args:
            field_type: 原始字段类型

        Returns:
            MySQL兼容类型
        """
        if not field_type:
            return 'varchar'

        type_mapping = {
            'BIGINT': 'bigint',
            'VARCHAR': 'varchar',
            'SMALLINT': 'smallint',
            'TINYINT': 'tinyint',
            'INT': 'int',
            'DECIMAL': 'decimal',
            'DATETIME': 'datetime',
            'TEXT': 'text',
            'LONGTEXT': 'longtext',
            'DATE': 'date',
            'TIME': 'time',
            'TIMESTAMP': 'timestamp',
            'FLOAT': 'float',
            'DOUBLE': 'double',
            'BOOLEAN': 'boolean'
        }

        # 转换为大写进行匹配
        upper_type = field_type.upper()
        return type_mapping.get(upper_type, upper_type.lower())

    def _parse_relations(self, table_id: str) -> List[Dict]:
        """
        解析表的关系信息

        Args:
            table_id: 表ID

        Returns:
            关系列表
        """
        relations = []

        for edge in self.edges:
            source_cell = edge.get('source', {}).get('cell')
            target_cell = edge.get('target', {}).get('cell')

            # 检查当前表是否参与这个关系
            if source_cell == table_id or target_cell == table_id:
                # 获取 target.port 和 source.port
                target_port = edge.get('target', {}).get('port', '')
                source_port = edge.get('source', {}).get('port', '')

                # 根据当前表是源还是目标来决定使用哪个端口
                if source_cell == table_id:
                    # 当前表是源表，使用 target.port
                    port_parts = target_port.split('-')
                    if len(port_parts) >= 3:
                        target = f"{port_parts[0]}_{port_parts[1]}"  # 第1、2部分拼接
                        condition = port_parts[2]  # 第3部分作为条件
                    else:
                        continue  # 如果端口格式不符合预期则跳过
                else:
                    # 当前表是目标表，使用 source.port
                    port_parts = source_port.split('-')
                    if len(port_parts) >= 3:
                        target = f"{port_parts[0]}_{port_parts[1]}"  # 第1、2部分拼接
                        condition = port_parts[2]  # 第3部分作为条件
                    else:
                        continue  # 如果端口格式不符合预期则跳过

                # 获取基数信息
                cardinality = edge.get('label', {}).get('text', '')

                # 根据目标表ID获取目标表名
                target_table_id = source_cell if source_cell != table_id else target_cell
                target_table_name = self._get_table_name_by_id(target_table_id)

                # 如果找不到目标表名，跳过此关系
                if not target_table_name:
                    continue

                # 生成ODS表名格式
                target_ods_name = self._generate_ods_table_name(port_parts[0], target_table_name)

                relation = {
                    "target": target_ods_name,  # 使用生成的ODS表名
                    "condition": condition,
                    "cardinality": cardinality
                }
                relations.append(relation)

        return relations

    def _get_table_name_by_id(self, table_id: str) -> Optional[str]:
        """
        根据表ID获取表名

        Args:
            table_id: 表ID

        Returns:
            表名或None
        """
        for table in self.tables:
            if table.get('id') == table_id:
                return table.get('tableName')
        return None


def parse_txt_to_json_files(txt_file_path: str, output_dir: str = "./metadata/ods") -> List[str]:
    """
    解析txt文件中的JSON并生成目标JSON文件

    Args:
        txt_file_path: 输入txt文件路径
        output_dir: 输出目录

    Returns:
        生成的文件路径列表
    """
    parser = TextJsonParser()
    results = parser.parse_text_file(txt_file_path)

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    generated_files = []

    # 为每个表生成单独的JSON文件
    for result in results:
        # 获取表名作为文件名
        table_name = list(result.keys())[0]  # 这是ODS格式的表名
        output_path = os.path.join(output_dir, f"{table_name}.json")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

        print(f"Generated: {output_path}")
        generated_files.append(output_path)

    return generated_files


# 使用示例
if __name__ == "__main__":
    # 解析示例
    file_paths = parse_txt_to_json_files("/Users/chenghongyuan/PycharmProjects/MyFirstPython/text_to_data_modeling/metadata/long_text_b71aba18-3f28-40da-8631-c71af428ccc9.txt",
                                         "tmp")
    # pass
