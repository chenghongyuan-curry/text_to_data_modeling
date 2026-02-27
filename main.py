import os
import shutil
from dotenv import load_dotenv
from app.core import AutoDWPipeline

# 加载环境变量
load_dotenv(override=True)

# 控制是否开启调试模式
DEBUG_MODE = True  # 开启调试模式，每次运行会清理 output 和 metadata/dwd/dws/ads/，并清空 Milvus
# DEBUG_MODE = False # 生产模式，不自动清理历史数据

def clear_generated_files(pipeline_instance):
    """清理历史产出物，便于调试"""
    if not DEBUG_MODE:
        print("[INFO] Debug mode is OFF. Skipping file cleanup.")
        return

    print("\n[DEBUG] Clearing generated files for a clean run (DEBUG_MODE is ON)...")
    
    # 定义要清理的目录
    output_dir = 'output'

    # 1. 清理 output 目录
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        print(f"  - Cleared directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # 2. 清理 metadata 子目录 (注意：严禁清理 source_db)
    # 现在 ods 也作为动态生成的层级，需要在 DEBUG 模式下被清理
    metadata_layers_to_clear = ['ods', 'dwd', 'dws', 'ads']
    for layer in metadata_layers_to_clear:
        layer_path = os.path.join('metadata', layer)
        if os.path.exists(layer_path):
            shutil.rmtree(layer_path)
            print(f"  - Cleared directory: {layer_path}")
        os.makedirs(layer_path, exist_ok=True)
    
    # 3. 清理 Milvus 向量数据库
    print("  - Clearing Milvus vector database...")
    pipeline_instance.km.reset_collection()
            
    print("[DEBUG] Workspace cleared.")

def main():
    # 初始化流水线
    try:
        pipeline = AutoDWPipeline()
    except Exception as e:
        print(f"[FATAL] Failed to initialize pipeline: {e}")
        return

    print("==================================================")
    print("   Text-To-Report 智能数仓开发助手 (Interactive Mode)   ")
    print("==================================================")

    if DEBUG_MODE:
        print("[INFO] DEBUG_MODE is ON. Automatic cleanup will occur.")
        # 首次清理，确保一个干净的初始环境
        clear_generated_files(pipeline)
    else:
        print("[INFO] DEBUG_MODE is OFF. Historical data will be preserved.")


    while True:
        # 获取用户输入
        try:
            user_query = input("\n📝 请输入开发需求 (输入 'exit' 或 'q' 退出，'clear' 清理): ").strip()
        except KeyboardInterrupt:
            print("\n[INFO] Exiting...")
            break

        if user_query.lower() in ['exit', 'q', 'quit']:
            print("[INFO] Goodbye!")
            break
        
        if user_query.lower() == 'clear':
            # 只有在 DEBUG_MODE 下才允许手动清理
            if DEBUG_MODE:
                clear_generated_files(pipeline)
            else:
                print("[WARNING] Debug mode is OFF. Manual cleanup is disabled. To enable, set DEBUG_MODE = True in main.py.")
            continue

        if not user_query:
            continue
            
        # 启动交互式流程
        try:
            # 每次运行前都清理，确保环境干净
            clear_generated_files(pipeline)
            pipeline.run_interactive(user_query)
        except Exception as e:
            print(f"[ERROR] Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
