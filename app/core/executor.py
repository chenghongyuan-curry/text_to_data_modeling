import os
import subprocess
import sys
from pathlib import Path
from clickhouse_driver import Client
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# DataX 路径
DATAX_HOME = Path(__file__).parent.parent.parent / "datax-0.0.1/datax"
DATAX_BIN = DATAX_HOME / "bin/datax.py"

class Executor:
    def __init__(self):
        # ClickHouse Config
        self.ck_host = os.environ.get("CLICKHOUSE_HOST")
        self.ck_port = int(os.environ.get("CLICKHOUSE_PORT", 9000))
        self.ck_user = os.environ.get("CLICKHOUSE_USER")
        self.ck_password = os.environ.get("CLICKHOUSE_PASSWORD")
        self.ck_db = os.environ.get("CLICKHOUSE_DB")
        
        # MySQL Config
        self.mysql_host = os.environ.get("MYSQL_HOST")
        self.mysql_port = os.environ.get("MYSQL_PORT")
        self.mysql_user = os.environ.get("MYSQL_USER")
        self.mysql_password = os.environ.get("MYSQL_PASSWORD")
        self.mysql_db = os.environ.get("MYSQL_DB")
        
        self.client = None

    def _get_ck_client(self):
        if not self.client:
            print(f"[INFO] Connecting to ClickHouse: {self.ck_host}:{self.ck_port} (User: {self.ck_user})")
            try:
                self.client = Client(
                    host=self.ck_host,
                    port=self.ck_port,
                    user=self.ck_user,
                    password=self.ck_password,
                    database=self.ck_db
                )
            except Exception as e:
                print(f"[ERROR] ClickHouse connection failed: {e}")
                raise
        return self.client

    def run_datax(self, job_path):
        """执行 DataX 任务"""
        job_path = Path(job_path).resolve()
        if not job_path.exists():
            print(f"[ERROR] DataX job file not found: {job_path}")
            return False

        print(f"\n[START] Executing DataX job: {job_path.name}")
        
        # 检查 datax.py 是否存在
        if not DATAX_BIN.exists():
            print(f"[ERROR] DataX execution script not found: {DATAX_BIN}")
            return False

        # 读取模板并替换参数
        try:
            content = job_path.read_text(encoding='utf-8')
            # 替换 MySQL 参数
            content = content.replace("${MYSQL_HOST}", self.mysql_host or "")
            content = content.replace("${MYSQL_PORT}", str(self.mysql_port or ""))
            content = content.replace("${MYSQL_USER}", self.mysql_user or "")
            content = content.replace("${MYSQL_PASSWORD}", self.mysql_password or "")
            content = content.replace("${MYSQL_DB}", self.mysql_db or "")
            
            # 替换 ClickHouse 参数
            content = content.replace("${CLICKHOUSE_HOST}", self.ck_host or "")
            content = content.replace("${CLICKHOUSE_PORT}", str(self.ck_port or ""))
            content = content.replace("${CLICKHOUSE_USER}", self.ck_user or "")
            content = content.replace("${CLICKHOUSE_PASSWORD}", self.ck_password or "")
            content = content.replace("${CLICKHOUSE_DB}", self.ck_db or "")
            
            # 写入临时运行文件
            temp_job_path = job_path.with_suffix('.running.json')
            temp_job_path.write_text(content, encoding='utf-8')
            print(f"  [INFO] Temporary configuration file generated: {temp_job_path.name}")
            
        except Exception as e:
            print(f"[ERROR] Configuration file processing failed: {e}")
            return False

        cmd = [sys.executable, str(DATAX_BIN), str(temp_job_path)]
        
        try:
            # 使用 subprocess 执行，实时输出日志
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                env=os.environ.copy() # 传递当前环境变量
            )
            
            for line in process.stdout:
                print(line, end='')
            
            process.wait()
            
            if process.returncode == 0:
                print(f"[SUCCESS] DataX job executed successfully.")
                return True
            else:
                print(f"[ERROR] DataX job execution failed (Exit Code: {process.returncode})")
                return False
                
        except Exception as e:
            print(f"[ERROR] Exception occurred during DataX execution: {e}")
            return False
        finally:
            # 清理临时文件
            if temp_job_path.exists():
                try:
                    os.remove(temp_job_path)
                    print(f"  [INFO] Temporary file cleaned up.")
                except:
                    pass

    def run_sql_file(self, sql_path):
        """执行 SQL 文件"""
        sql_path = Path(sql_path).resolve()
        if not sql_path.exists():
            print(f"[ERROR] SQL file not found: {sql_path}")
            return False

        print(f"\n[START] Executing SQL script: {sql_path.name}")
        client = self._get_ck_client()
        
        try:
            with open(sql_path, 'r') as f:
                sql_content = f.read()
            
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            for i, stmt in enumerate(statements):
                print(f"  [INFO] Executing statement {i+1}/{len(statements)}...")
                print(f"     {stmt[:100].replace(chr(10), ' ')}...") 
                
                client.execute(stmt)
            
            print(f"[SUCCESS] SQL script executed successfully.")
            return True
            
        except Exception as e:
            print(f"[ERROR] SQL execution failed: {e}")
            return False

if __name__ == "__main__":
    # 测试代码
    exe = Executor()