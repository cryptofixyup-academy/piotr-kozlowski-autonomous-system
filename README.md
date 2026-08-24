# Piotr Kozlowski Autonomous System

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-active-success.svg)]()

> Modular, production-ready framework for autonomous agents across logistics, trading, and crypto forensics.

The **Piotr Kozlowski Autonomous System** is a collection of three independent, config-driven AI pipelines designed for real operational workloads. Each package is self-contained, observable, and can run standalone or be orchestrated together.

---

## Architecture
piotr-kozlowski-autonomous-system/
├─ logi_agent_package/          # Logistics optimization
│  ├─ logi_agent_pipeline.py
│  ├─ config.json
│  ├─ mock_orders.json
│  ├─ requirements.txt
│  └─ README.md
├─ trading_ai_package/          # Trading strategy & backtesting
│  ├─ trading_ai_pipeline.py
│  ├─ config.json
│  ├─ mock_market_data.json
│  ├─ requirements.txt
│  └─ README.md
└─ crypto_forensics_package/    # On-chain analysis
   ├─ forensics_pipeline.py
   ├─ config.json
   ├─ requirements.txt
   └─ README.md
   
---

## Modules

### 1. Logi-Agent Package
Autonomous logistics optimization.

**Capabilities:**
- Fetches new orders from source systems
- Assigns vehicles & carriers using constraint-based matching
- Calculates margin, detects backhaul opportunities and load consolidation
- Updates dashboard metrics in real-time

**Run:**
```bash
cd logi_agent_package
pip install -r requirements.txt
python logi_agent_pipeline.py
