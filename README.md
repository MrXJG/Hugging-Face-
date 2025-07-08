# Hugging Face 数据集下载工具

![Python Version](https://img.shields.io/badge/python-3.7+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

一个简单高效的Hugging Face数据集下载工具，支持交互式搜索、下载和格式转换。

## ✨ 功能特性

- 🔍 交互式搜索Hugging Face数据集
- ⚡ 多线程下载加速
- 🔄 自动重试机制（网络不稳定时特别有用）
- 📁 支持多种输出格式：CSV/JSON/Parquet
- 🎯 支持数据采样（按指定条数下载）
- 🛡️ 代理支持（适合国内用户）

## 📦 安装使用

### 前置要求
- Python 3.7+
- pip

### 安装依赖
```bash
pip install pandas datasets huggingface-hub
```

### 快速开始
```bash
python hf_downloader.py
```

## 🖥️ 使用示例

### 交互式模式
```bash
请输入搜索关键词（如 'news' 或 'ag_news'）：news

📂 匹配的数据集：
1. fancyzhx/ag_news
2. SetFit/ag_news
...

请输入要下载的编号 (1-5)：1
保存格式 (csv/json/parquet，默认csv)：json
保存目录 (默认 './data')：./my_data
采样条数（留空下载全部）：1000

✅ 下载完成!
✅ 格式转换完成!
```

### 编程方式使用
```python
from hf_downloader import download_dataset

result = download_dataset(
    dataset_name="fancyzhx/ag_news",
    save_format="csv",
    sample=500
)
print(result)
```

## ⚙️ 配置选项

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `HTTP_PROXY` | 设置HTTP代理 | 无 |
| `HTTPS_PROXY` | 设置HTTPS代理 | 无 |
| `MAX_RETRIES` | 最大重试次数 | 3 |
| `TIMEOUT` | 请求超时(秒) | 30 |

## 🤝 参与贡献

欢迎提交Issue和PR！贡献流程：
1. Fork本项目
2. 创建新分支 
3. 提交更改 
4. 推送到分支 
5. 创建Pull Request

## 📜 开源协议

本项目采用 [MIT License](LICENSE)

## 🙏 致谢

- 感谢Hugging Face提供优秀的开源平台
- 感谢所有贡献者和用户

---

> 💡 提示：首次使用建议先尝试小数据集测试功能是否正常
