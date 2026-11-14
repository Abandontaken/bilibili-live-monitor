"""主播侧礼物价值：盲盒按结果单价乘数量，原始投入字段不变。"""

VALUE_BASIS = 'gift-output-v1'


def value_fields(data):
    """给新计算的聚合增加明确的面值字段；不用于给旧快照改标签。"""
    if isinstance(data, list):
        for row in data:
            value_fields(row)
    elif isinstance(data, dict):
        for row in list(data.values()):
            value_fields(row)
        if 'gift_amount_yuan' in data:
            data['gift_value_yuan'] = data['gift_amount_yuan']
            data['gift_value_basis'] = VALUE_BASIS
    return data


def integer(value):
    return value if type(value) is int and value >= 0 else None


def is_blind(gift):
    return isinstance(gift.get('blind_gift'), dict)


def gift_value(gift):
    """返回千分元；未知为None，银币为0，平台总额不再乘数量。"""
    if gift.get('event_type') == 'SUPER_CHAT_MESSAGE':
        price = integer(gift.get('price_yuan'))
        return price * 1000 if price is not None else integer(gift.get('total_coin'))
    if gift.get('event_type') == 'GUARD_BUY':
        return integer(gift.get('guard_price_gold'))
    if gift.get('coin_type') == 'silver':
        return 0
    if gift.get('coin_type') != 'gold':
        return None
    price, count = integer(gift.get('price')), integer(gift.get('num'))
    if price is not None and count is not None and count > 0:
        return price * count
    # 普通直送礼物的历史记录可使用平台总额；盲盒绝不回退到投入价。
    return None if is_blind(gift) else integer(gift.get('total_coin'))


def gift_text(gift, original=''):
    """根据结构化字段渲染，历史存储的错误金额文案不进入页面。"""
    if gift.get('event_type') not in ('SEND_GIFT', 'SEND_GIFT_V2'):
        return original
    name, count = gift.get('gift_name'), integer(gift.get('num'))
    if not name or not count:
        return f"{'盲盒结果：' if is_blind(gift) else '赠送 '}{name or '未知礼物'} · 数量未知 · 礼物价值未知"
    text = f"{'盲盒结果：' if is_blind(gift) else '赠送 '}{name} ×{count}"
    if gift.get('coin_type') == 'silver':
        total = integer(gift.get('total_coin'))
        return text + (f' · {total} 银币' if total is not None else '')
    value = gift_value(gift)
    return text + (f' · 礼物价值 ¥{value / 1000:,.2f}' if value is not None else ' · 礼物价值未知')
