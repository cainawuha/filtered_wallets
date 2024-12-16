import pandas as pd
import requests
import time

# 配置 Alchemy RPC
SOLANA_RPC_URL = "https://solana-mainnet.g.alchemy.com/v2/你自己的api key"

# 输入 CSV 文件路径
file_path = r"F:\币圈小程序\2024-12-14-01.csv"
wallet_data = pd.read_csv(file_path)  # 加载文件，第一行为列名

def get_signatures(wallet_address, limit=100, retries=5):
    """
    获取指定钱包的交易签名列表
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [wallet_address, {"limit": limit}],
    }
    for attempt in range(retries):
        try:
            response = requests.post(SOLANA_RPC_URL, json=payload, timeout=10)
            data = response.json()
            return [sig['signature'] for sig in data.get('result', [])]
        except Exception as e:
            print(f"Error fetching signatures for {wallet_address}, attempt {attempt + 1}: {e}")
            time.sleep(2 ** attempt)  # 指数回退
    return []

def get_transaction(signature, retries=5):
    """
    获取指定交易签名的交易详情
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, "jsonParsed"],
    }
    for attempt in range(retries):
        try:
            response = requests.post(SOLANA_RPC_URL, json=payload, timeout=10)
            data = response.json()
            return data.get('result')
        except Exception as e:
            print(f"Error fetching transaction {signature}, attempt {attempt + 1}: {e}")
            time.sleep(2 ** attempt)  # 指数回退
    return None

def analyze_wallet(wallet):
    """
    分析单个钱包，判断是否为高频交易
    """
    signatures = get_signatures(wallet)
    transactions = []

    for signature in signatures:
        tx = get_transaction(signature)
        if not tx:
            continue

        instructions = tx.get('transaction', {}).get('message', {}).get('instructions', [])
        for instr in instructions:
            if instr.get('programId') == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
                parsed = instr.get('parsed', {}).get('info', {})
                transactions.append({
                    "token": parsed.get("mint"),
                    "type": "buy" if parsed.get("destination") == wallet else "sell",
                    "timestamp": tx.get("blockTime"),
                })
        time.sleep(0.1)

    tokens = {}
    for tx in transactions:
        token = tx["token"]
        if token not in tokens:
            tokens[token] = []
        tokens[token].append(tx)

    high_frequency_tokens = 0
    for token, txs in tokens.items():
        buy_sell_pairs = sorted(txs, key=lambda x: x["timestamp"])
        intervals = [
            buy_sell_pairs[i + 1]["timestamp"] - buy_sell_pairs[i]["timestamp"]
            for i in range(len(buy_sell_pairs) - 1)
        ]
        if len(intervals) > 0 and sum(1 for interval in intervals if interval < 60) / len(intervals) >= 0.5:
            high_frequency_tokens += 1

    return high_frequency_tokens / len(tokens) < 0.5 if tokens else False

def filter_wallets(data):
    """
    筛选符合条件的钱包，并保留所有列
    """
    filtered_data = []
    for _, row in data.iterrows():
        wallet = row['wallet']
        print(f"Analyzing wallet: {wallet}")
        try:
            if analyze_wallet(wallet):
                filtered_data.append(row)
        except Exception as e:
            print(f"Error analyzing wallet {wallet}: {e}")
    return pd.DataFrame(filtered_data)

# 筛选钱包，保留所有列
filtered_wallet_data = filter_wallets(wallet_data)

# 保存结果到新的 CSV 文件
output_path = r"F:\币圈小程序\filtered_wallets.csv"
filtered_wallet_data.to_csv(output_path, index=False, encoding='utf-8-sig')  # 确保编码兼容性
print(f"Filtered wallets saved to {output_path}")

