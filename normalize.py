from typing import List, Dict, Any

from helpers import (
    to_iso,
    wei_to_eth,
    token_amount,
    direction,
    protocol_label_for_address,
)


def normalize_for_csv(
    address: str,
    native_txs: List[Dict[str, Any]],
    token_txs: List[Dict[str, Any]],
    nft_txs: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    address = address.lower()
    rows: List[Dict[str, Any]] = []

    # Native Base transactions
    for tx in native_txs:
        from_addr = tx.get("from", "")
        to_addr = tx.get("to", "")

        gas_used = tx.get("gasUsed") or tx.get("gas", "0")
        gas_price = tx.get("gasPrice", "0")

        try:
            fee_wei = int(gas_used) * int(gas_price)
        except Exception:
            fee_wei = 0

        protocol = protocol_label_for_address(to_addr) or protocol_label_for_address(
            from_addr
        )

        rows.append(
            {
                "hash": tx.get("hash", ""),
                "tx_type": "base_native",
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": tx.get("timeStamp", ""),
                "timeStamp_iso": to_iso(tx.get("timeStamp", "")),
                "from": from_addr,
                "to": to_addr,
                "direction": direction(address, from_addr, to_addr),
                "protocol": protocol,
                "activity_type": "",  # will fill later
                "native_amount_eth": wei_to_eth(tx.get("value", "0")),
                "token_contract": "",
                "token_symbol": "",
                "token_name": "",
                "token_decimals": "",
                "token_amount": "",
                "gas": tx.get("gas", ""),
                "gasPrice": gas_price,
                "gasUsed": gas_used,
                "tx_fee_eth": wei_to_eth(str(fee_wei)),
                "token_in_assets": "",
                "token_out_assets": "",
            }
        )

    # Token transfers (DeFi)
    for tx in token_txs:
        from_addr = tx.get("from", "")
        to_addr = tx.get("to", "")
        decimals = tx.get("tokenDecimal", "0")
        contract_addr = tx.get("contractAddress", "")

        protocol = (
            protocol_label_for_address(to_addr)
            or protocol_label_for_address(from_addr)
            or protocol_label_for_address(contract_addr)
        )

        rows.append(
            {
                "hash": tx.get("hash", ""),
                "tx_type": "base_token_transfer",
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": tx.get("timeStamp", ""),
                "timeStamp_iso": to_iso(tx.get("timeStamp", "")),
                "from": from_addr,
                "to": to_addr,
                "direction": direction(address, from_addr, to_addr),
                "protocol": protocol,
                "activity_type": "",  # will fill later
                "native_amount_eth": "",
                "token_contract": contract_addr,
                "token_symbol": tx.get("tokenSymbol", ""),
                "token_name": tx.get("tokenName", ""),
                "token_decimals": decimals,
                "token_amount": token_amount(tx.get("value", "0"), decimals),
                "gas": "",
                "gasPrice": "",
                "gasUsed": "",
                "tx_fee_eth": "",
                "token_in_assets": "",
                "token_out_assets": "",
            }
        )

    # NFT transfers (ERC-721) for position NFTs (e.g., Aerodrome v3 LPs)
    for tx in nft_txs:
        from_addr = tx.get("from", "")
        to_addr = tx.get("to", "")
        contract_addr = tx.get("contractAddress", "")

        protocol = (
            protocol_label_for_address(to_addr)
            or protocol_label_for_address(from_addr)
            or protocol_label_for_address(contract_addr)
        )

        rows.append(
            {
                "hash": tx.get("hash", ""),
                "tx_type": "base_nft_transfer",
                "blockNumber": tx.get("blockNumber", ""),
                "timeStamp": tx.get("timeStamp", ""),
                "timeStamp_iso": to_iso(tx.get("timeStamp", "")),
                "from": from_addr,
                "to": to_addr,
                "direction": direction(address, from_addr, to_addr),
                "protocol": protocol,
                "activity_type": "",  # will fill later
                "native_amount_eth": "",
                "token_contract": contract_addr,
                "token_symbol": tx.get("tokenSymbol", ""),
                "token_name": tx.get("tokenName", ""),
                "token_decimals": "",
                "token_amount": 1,  # NFT transfer represents 1 position token
                "gas": "",
                "gasPrice": "",
                "gasUsed": "",
                "tx_fee_eth": "",
                "token_in_assets": "",
                "token_out_assets": "",
            }
        )

    # Sort chronologically
    def sort_key(r: Dict[str, Any]):
        try:
            return (int(r.get("blockNumber", "0")), int(r.get("timeStamp", "0")))
        except Exception:
            return (0, 0)

    rows.sort(key=sort_key)
    return rows
