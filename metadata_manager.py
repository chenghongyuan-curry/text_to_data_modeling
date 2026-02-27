import json
import os
from pathlib import Path

# 获取当前脚本所在目录
BASE_DIR = Path(__file__).parent

# 1. ODS 层元数据：侧重物理存储、数据源信息、DataX 配置
ods_meta = {
    "ods_sy_order_db_order": {
        "source": {"type": "mysql", "db": "order_db", "table": "t_order"},
        "columns": [
            {"name": "id", "type": "bigint", "is_primary": True},
            {"name": "status", "type": "varchar(32)"},
            {"name": "is_deleted", "type": "tinyint"}
        ]
    }
}

# 2. DWD 层元数据：侧重业务逻辑、枚举映射、关联关系
dwd_meta = {
    "dwm_mall_po_subject_item_df": {
        "base_table": "ods_sy_order_db_order_item",
        "joins": [
            {"table": "ods_sy_order_db_order", "on": "order_id=id", "type": "inner"}
        ],
        "mappings": {
            "status": {
                "DONE": "已完成",
                "CANCEL": "已取消"
            }
        }
    }
}

# 3. ADS 层元数据：侧重业务指标定义、报表口径
ads_meta = {
    "ads_purchase_efficiency_report": {
        "source_models": ["dwm_mall_po_subject_item_df"],
        "metrics": [
            {"name": "total_amount", "formula": "sum(tax_included_amount)", "desc": "总金额"}
        ]
    }
}

def save_metadata():
    metadata_dir = BASE_DIR / "metadata"
    metadata_dir.mkdir(exist_ok=True)
    
    with open(metadata_dir / "ods_meta.json", "w") as f: json.dump(ods_meta, f, indent=4)
    with open(metadata_dir / "dwd_meta.json", "w") as f: json.dump(dwd_meta, f, indent=4)
    with open(metadata_dir / "ads_meta.json", "w") as f: json.dump(ads_meta, f, indent=4)

if __name__ == "__main__":
    save_metadata()
    print(f"✅ 分层元数据已成功拆分并存储在 {BASE_DIR}/metadata 目录下。")
