import pandas as pd ,os ,time ,requests ,warnings
from datasets import load_dataset
from huggingface_hub import list_datasets, HfApi, snapshot_download
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置参数
TIMEOUT = 30  # 秒
MAX_RETRIES = 3
RETRY_DELAY = 5.0  # 基础重试间隔
MAX_WORKERS = 2  # 最大并发数

# 设置代理（可按需注释）
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:7897"
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7897"

def robust_operation(operation, *args, **kwargs):
    """通用重试装饰器"""
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"⚠️ 操作失败 (尝试 {attempt + 1}/{MAX_RETRIES}), {wait_time:.1f}秒后重试...")
                time.sleep(wait_time)
    raise last_error

def search_datasets(keyword: str, top_k: int = 5) -> list:
    """搜索包含关键词的数据集（带超时和重试）"""
    print(f"🔍 正在搜索数据集 '{keyword}'...")
    api = HfApi()
    
    def _search():
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future = executor.submit(
                api.list_datasets,
                search=keyword,
                limit=top_k*2
            )
            return future.result(timeout=TIMEOUT)
    
    try:
        datasets = robust_operation(_search)
        matched = []
        seen = set()
        for ds in datasets:
            if keyword.lower() in ds.id.lower() and ds.id not in seen:
                seen.add(ds.id)
                matched.append(ds.id)
            if len(matched) >= top_k:
                break
        return matched[:top_k]
    except Exception as e:
        raise Exception(f"搜索失败: {str(e)}")

def download_dataset(
    dataset_name: str,
    output_dir: str = "./data",
    download_all: bool = True,
    save_format: Optional[str] = None,
    sample: Optional[int] = None
) -> Dict:
    """
    优化显示版本的数据集下载
    """
    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)
    result = {"saved_files": []}

    try:
        # 下载原始文件（带静默重试）
        if download_all:
            print(f"📥 正在下载数据集 [{dataset_name}]...", end="", flush=True)
            
            # 禁用huggingface的警告
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                repo_path = robust_operation(
                    snapshot_download,
                    repo_id=dataset_name,
                    repo_type="dataset",
                    local_dir=os.path.join(output_dir, dataset_name.replace('/', '_')),
                    resume_download=True,
                    max_workers=MAX_WORKERS,
                    allow_patterns=["*"],
                    ignore_patterns=["*.lock", "*.tmp"]
                )
            
            print("\r✅ 下载完成!" + " " * 30)  # 清除进度行
            result["saved_files"].append(repo_path)

        # 数据转换
        if save_format:
            print(f"🔄 正在转换格式 ({save_format.upper()})...", end="", flush=True)
            dataset = robust_operation(load_dataset, dataset_name)
            
            for split_name, data in dataset.items():
                df = data.to_pandas()
                if sample:
                    df = df.sample(min(sample, len(df)))
                
                filename = f"{dataset_name.replace('/', '_')}_{split_name}.{save_format}"
                save_path = os.path.join(output_dir, filename)
                
                save_func = {
                    "csv": lambda: df.to_csv(save_path, index=False),
                    "json": lambda: df.to_json(save_path, orient="records"),
                    "parquet": lambda: df.to_parquet(save_path)
                }.get(save_format)
                
                if save_func:
                    robust_operation(save_func)
                    result["saved_files"].append(save_path)
            
            print("\r✅ 格式转换完成!" + " " * 30)

        result.update({
            "status": "success",
            "time_used": f"{time.time() - start_time:.1f}s"
        })
        
    except Exception as e:
        print("\r❌ 操作失败!" + " " * 30)  # 清除进度行
        result.update({
            "status": "error",
            "message": str(e),
            "time_used": f"{time.time() - start_time:.1f}s"
        })
    
    return result


def interactive_download():
    """交互式下载流程（保持不变）"""
    print("\n" + "="*50)
    print("🔍 Hugging Face 数据集下载工具 (v2.1)")
    print("="*50)
    
    try:
        keyword = input("\n请输入搜索关键词（如 'news' 或 'ag_news'）：").strip()
        if not keyword:
            raise ValueError("搜索关键词不能为空")
        
        matched_datasets = search_datasets(keyword)
        if not matched_datasets:
            print("⚠️ 未找到匹配的数据集")
            return
        
        print("\n📂 匹配的数据集：")
        for i, ds in enumerate(matched_datasets, 1):
            print(f"{i}. {ds}")
        
        choice = input(f"\n请输入要下载的编号 (1-{len(matched_datasets)})：")
        try:
            selected = matched_datasets[int(choice)-1]
        except (ValueError, IndexError):
            print("❌ 无效的选择，请输入有效编号")
            return
        
        save_format = input("保存格式 (csv/json/parquet，默认csv)：").strip().lower() or "csv"
        if save_format not in ["csv", "json", "parquet"]:
            print("❌ 不支持的格式，将使用CSV")
            save_format = "csv"
        
        output_dir = input(f"保存目录 (默认 './data')：").strip() or "./data"
        sample = input("采样条数（留空下载全部）：").strip()
        sample = int(sample) if sample.isdigit() else None
        
        result = download_dataset(
            dataset_name=selected,
            save_format=save_format,
            output_dir=output_dir,
            sample=sample
        )
        
        if result["status"] == "error":
            print(f"\n❌ 下载失败: {result['message']}")
        else:
            print("\n🎉 操作完成！")
            print(f"📁 保存位置: {result['saved_files']}")
            print(f"⏱️ 总耗时: {result['time_used']}")
    
    except KeyboardInterrupt:
        print("\n🛑 用户中断操作")
    except Exception as e:
        print(f"\n❌ 发生错误: {str(e)}")

if __name__ == "__main__":
    interactive_download()
