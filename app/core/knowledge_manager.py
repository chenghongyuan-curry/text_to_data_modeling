import os
import json
from typing import List, Dict, Any
from pymilvus import (
    connections,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    utility
)
from dotenv import load_dotenv
from .engine import AutoDWEngine

load_dotenv(override=True)

class KnowledgeManager:
    def __init__(self):
        self.host = os.environ.get("MILVUS_HOST", "localhost")
        self.port = os.environ.get("MILVUS_PORT", "19530")
        self.user = os.environ.get("MILVUS_USER", "")
        self.password = os.environ.get("MILVUS_PASSWORD", "")
        self.collection_name = "dw_metadata_v1"
        
        self.engine = AutoDWEngine()
        
        # Adjust dimension based on provider
        if self.engine.provider == "aliyun":
            self.dim = 1536
        elif self.engine.provider == "google":
            self.dim = 768
        else:
            self.dim = 768
        
        self._connect()
        self._init_collection()

    def _connect(self):
        try:
            print(f"[INFO] Connecting to Milvus {self.host}:{self.port}...")
            connections.connect(
                "default",
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            print("[SUCCESS] Milvus Connected.")
        except Exception as e:
            print(f"[ERROR] Milvus Connection Failed: {e}")
            raise

    def _init_collection(self):
        """Initialize Milvus collection if not exists"""
        if utility.has_collection(self.collection_name):
            self.collection = Collection(self.collection_name)
            self.collection.load()
            return

        print(f"[INFO] Creating collection: {self.collection_name} (dim={self.dim})")
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535), # Raw metadata text
            FieldSchema(name="metadata_json", dtype=DataType.VARCHAR, max_length=65535), # Original JSON string
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=self.dim)
        ]
        schema = CollectionSchema(fields, "Data Warehouse Metadata Storage")
        self.collection = Collection(self.collection_name, schema)
        
        # Create Index
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128}
        }
        self.collection.create_index(field_name="embedding", index_params=index_params)
        self.collection.load()
        print("[SUCCESS] Collection initialized.")

    def reset_collection(self):
        """Drop existing collection and recreate it for a clean start"""
        if utility.has_collection(self.collection_name):
            print(f"[INFO] Dropping existing collection: {self.collection_name} to clear history...")
            utility.drop_collection(self.collection_name)
        self._init_collection()

    def ingest_metadata(self, metadata_root: str):
        """递归扫描 metadata 目录并入库 Milvus"""
        print(f"\n[INFO] Scanning metadata directory: {metadata_root}")
        data_rows = []
        
        # 递归查找所有 .json 文件
        for root, dirs, files in os.walk(metadata_root):
            for file in files:
                if not file.endswith('.json'):
                    continue
                
                path = os.path.join(root, file)
                try:
                    with open(path, 'r') as f:
                        meta_dict = json.load(f)
                        
                    for table_name, meta in meta_dict.items():
                        # 构建深度 Embedding 描述
                        layer = meta.get('layer', 'UNKNOWN')
                        logic = meta.get('logic_summary', meta.get('description', '无业务描述'))
                        
                        # 核心语义：表名 + 业务逻辑描述 (作为索引的最核心部分)
                        desc_segments = [
                            f"Table Name: {table_name}",
                            f"Table Business Comment: {logic}", # 显式标注这是表注释
                            f"Data Layer: {layer}"
                        ]
                        
                        # 关键语义：字段名 + 字段注释
                        if "columns" in meta:
                            col_info = []
                            for c in meta["columns"]:
                                col_name = c.get("name", "")
                                col_comment = c.get("comment", "")
                                if col_comment:
                                    col_info.append(f"{col_name}({col_comment})")
                                else:
                                    col_info.append(col_name)
                            desc_segments.append(f"Fields Detail: {', '.join(col_info)}")
                        
                        # 关联语义：血缘关系
                        if "base_table" in meta:
                            desc_segments.append(f"Source Table: {meta['base_table']}")
                        if "joins" in meta:
                            joins = [j.get("table", "") for j in meta["joins"]]
                            desc_segments.append(f"Related Tables: {', '.join(joins)}")
                        
                        # 组合最终索引文本
                        full_desc = ". ".join(desc_segments)
                        
                        print(f"  [INFO] Indexing: {table_name} (Semantics depth increased)")
                        embedding = self.engine.embed_text(full_desc)
                        
                        data_rows.append({
                            "text": full_desc,
                            "metadata_json": json.dumps({table_name: meta}),
                            "embedding": embedding
                        })
                except Exception as e:
                    print(f"  [ERROR] Failed to process {path}: {e}")

        if data_rows:
            self.collection.insert([
                [row["text"] for row in data_rows],
                [row["metadata_json"] for row in data_rows],
                [row["embedding"] for row in data_rows]
            ])
            self.collection.flush()
            print(f"[SUCCESS] Ingested {len(data_rows)} tables from directory into Milvus.")
        else:
            print("[WARNING] No metadata files found to ingest.")

    def search_related_tables(self, query: str, top_k: int = 5) -> Dict[str, Any]:
        """Search for relevant tables based on user query"""
        print(f"\n[INFO] Searching knowledge base for: '{query}'")
        query_embedding = self.engine.embed_text(query)
        
        results = self.collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=top_k,
            output_fields=["metadata_json"]
        )
        
        combined_meta = {}
        for hits in results:
            for hit in hits:
                meta_json = hit.entity.get("metadata_json")
                combined_meta.update(json.loads(meta_json))
                
        print(f"  [INFO] Found {len(combined_meta)} relevant tables.")
        return combined_meta

if __name__ == "__main__":
    # Test
    km = KnowledgeManager()