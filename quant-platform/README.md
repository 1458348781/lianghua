# 可视化量化平台

这是按 `可视化量化平台基础框架方案.md` 落地的第一版本地量化研究平台，目标是先跑通：

`国内行情数据 -> SQLite 缓存 -> 策略信号 -> T+1 模拟成交 -> 账户净值 -> 指标与图表`

## 已实现能力

- 数据层：日线行情下载、清洗、SQLite 缓存、统一查询接口。
- 数据源：优先支持 AkShare（可选安装），内置东方财富公开 K 线接口作为免登录备选。
- 策略层：均线择时策略、动量月度调仓策略。
- 策略层：新增“分歧战法”，支持涨停分歧确认、T+1 盘中涨跌幅过滤、止盈止损和到期退出。
- 回测层：多股票日线回测、目标仓位下单、手续费、滑点、卖出印花税、100 股整数手。
- 分析层：累计收益、年化收益、最大回撤、夏普、波动率、胜率、交易次数。
- 可视化层：中文名/代码搜索股票、选择策略、查看净值曲线、回撤、月度收益、赚亏明细和历史 K 线。

## 快速启动

```powershell
cd E:\lianghua\quant-platform
.\run.ps1
```

启动后打开：

```text
http://127.0.0.1:8765
```

如果当前环境没有网络，下载真实行情会失败；可以先放入自己的 CSV，再通过后续扩展导入。真实数据下载不需要 token。

## 数据源说明

默认 `auto` 模式：

1. 如果安装了 `akshare`，优先使用 AkShare 的 A 股历史日线接口。
2. 如果没有安装 AkShare，则使用东方财富公开 K 线接口。

可选安装：

```powershell
& 'D:\anconda3\python.exe' -m pip install akshare
```

批量下载当前非 ST A 股从 2020 年至今的日线数据：

```powershell
cd E:\lianghua\quant-platform
& 'D:\anconda3\python.exe' .\scripts\download_non_st_baostock.py --start-date 2020-01-01 --end-date 2026-05-09
```

这个脚本使用 BaoStock 免费数据，支持断点续跑，下载过程中会把股票基础信息和日线行情写入 `data/database/market.sqlite`。

## API 摘要

- `GET /api/health`
- `GET /api/strategies`
- `GET /api/stocks/search?q=平安`
- `POST /api/data/download`
- `GET /api/market-data/daily?symbol=000001.SZ&start_date=2020-01-01&end_date=2024-12-31`
- `POST /api/backtests`
- `GET /api/backtests/{id}`

## 回测假设

- 使用日线前复权价格。
- T 日收盘后生成目标仓位，T+1 日开盘成交。
- 不处理涨跌停、停牌、幸存者偏差和分红现金流。
- A 股按 100 股整数手成交。
- 手续费、滑点、印花税按固定比例近似。

这些假设会在页面上显示，避免把原型回测误解成实盘表现。
