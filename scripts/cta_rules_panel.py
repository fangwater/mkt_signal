"""CTA rules 配置面板 — 仅供 intra_config_server 在 namespace=cta 的 env 下使用。

Redis 模型（与 crates/trade_signal/src/cta_config.rs 对齐）：
- `{env}:cta_rules` STRING，JSON 数组，元素是 RawCtaRule 形状的对象
- 校验逻辑复用 sync_cta_rules.validate_rules（与 Rust loader 同规则）

面板只在 BOOTSTRAP.features.cta_rules 为真（即 env 目录名含 -cta-）时展示。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import sync_cta_rules

# (name, group, type, label, hint)
# type: text | number | int | bool | select | offsets
RULE_FIELDS: List[Dict[str, Any]] = [
    {"name": "rule_id", "group": "identity", "type": "text", "label": "rule_id", "hint": "[a-zA-Z0-9_-]{1,32}，唯一"},
    {"name": "model_service", "group": "identity", "type": "text", "label": "model_service", "hint": "model_output/<service> 的 service 段"},
    {"name": "enabled", "group": "identity", "type": "bool", "label": "enabled", "hint": "禁用后该规则不产生信号"},
    {"name": "trade_sides", "group": "identity", "type": "select", "label": "trade_sides", "options": ["both", "long", "short"], "hint": "规则允许的方向"},
    {"name": "long_quantile", "group": "signal", "type": "number", "label": "long_quantile", "step": "0.01", "hint": "score 分位 > 此值做多"},
    {"name": "short_quantile", "group": "signal", "type": "number", "label": "short_quantile", "step": "0.01", "hint": "score 分位 < 此值做空"},
    {"name": "application", "group": "signal", "type": "select", "label": "application", "options": ["each_bar", "on_change"], "hint": "each_bar 每根 bar 评估 / on_change 只在方向翻转时"},
    {"name": "frequency_seconds", "group": "signal", "type": "int", "label": "frequency_seconds", "hint": "bar 周期（秒）"},
    {"name": "cooldown_seconds", "group": "signal", "type": "int", "label": "cooldown_seconds", "hint": "同 (rule,symbol) 两次开仓最小间隔，0=不限制"},
    {"name": "signal_delay_seconds", "group": "signal", "type": "int", "label": "signal_delay_seconds", "hint": "信号确认延迟（秒）"},
    {"name": "spread_long_quantile", "group": "spread", "type": "number", "label": "spread_long_quantile", "step": "0.01", "hint": "做多要求 spread_rate 分位 < 此值"},
    {"name": "spread_short_quantile", "group": "spread", "type": "number", "label": "spread_short_quantile", "step": "0.01", "hint": "做空要求 spread_rate 分位 > 此值"},
    {"name": "spread_cancel_quantile", "group": "spread", "type": "number", "label": "spread_cancel_quantile", "step": "0.01", "hint": "spread 分位越过此值撤同向未成交单"},
    {"name": "rolling_window", "group": "spread", "type": "int", "label": "rolling_window", "hint": "spread 滚动窗口长度（bar 数）"},
    {"name": "rolling_min_periods", "group": "spread", "type": "int", "label": "rolling_min_periods", "hint": "分位就绪所需最少样本 ∈ [1, rolling_window]"},
    {"name": "order_notional_usdt", "group": "exec", "type": "number", "label": "order_notional_usdt", "hint": "单档开仓名义（USDT）"},
    {"name": "open_offsets", "group": "exec", "type": "offsets", "label": "open_offsets", "hint": "逗号分隔的网格偏移（0..0.01），档数=个数"},
    {"name": "open_ttl_seconds", "group": "exec", "type": "int", "label": "open_ttl_seconds", "hint": "开仓挂单 TTL（秒）"},
    {"name": "max_position_notional_usdt", "group": "exec", "type": "number", "label": "max_position_notional_usdt", "hint": "单向名义上限，须 ≥ 档数×单档名义"},
    {"name": "take_profit", "group": "exit", "type": "number", "label": "take_profit", "step": "0.001", "hint": "swap 腿 maker 止盈偏移；0=不挂"},
    {"name": "reward_risk_ratio", "group": "exit", "type": "number", "label": "reward_risk_ratio", "step": "0.1", "hint": "stop = tp/rr"},
    {"name": "trailing_stop_enabled", "group": "exit", "type": "bool", "label": "trailing_stop_enabled", "hint": "启用 trailing stop"},
    {"name": "trailing_stop_trigger_step", "group": "exit", "type": "number", "label": "trailing_stop_trigger_step", "step": "0.0001", "hint": "trailing 触发步进"},
    {"name": "trailing_stop_move_step", "group": "exit", "type": "number", "label": "trailing_stop_move_step", "step": "0.0001", "hint": "trailing 移动步进"},
    {"name": "max_holding_seconds", "group": "exit", "type": "int", "label": "max_holding_seconds", "hint": "最长持仓（秒），0=不限制"},
]

GROUP_LABELS = {
    "identity": "标识",
    "signal": "信号分位",
    "spread": "Spread Overlay",
    "exec": "开仓执行",
    "exit": "退出/止损",
}


def cta_rules_redis_key(env_name: str) -> str:
    return sync_cta_rules.cta_rules_key(env_name)


def validate_cta_rules(rules: Any) -> List[str]:
    return sync_cta_rules.validate_rules(rules)


def read_cta_rules(rds, env_name: str) -> Dict[str, Any]:
    """读取 {env}:cta_rules。返回 {key, exists, count, rules, parse_error?}。"""
    key = cta_rules_redis_key(env_name)
    raw = rds.get(key)
    if raw is None:
        return {"key": key, "exists": False, "count": 0, "rules": []}
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        rules = json.loads(text)
    except Exception as exc:
        return {
            "key": key,
            "exists": True,
            "count": 0,
            "rules": [],
            "parse_error": f"{exc}",
            "raw": text,
        }
    if not isinstance(rules, list):
        return {
            "key": key,
            "exists": True,
            "count": 0,
            "rules": [],
            "parse_error": "stored value is not a JSON array",
            "raw": text,
        }
    return {"key": key, "exists": True, "count": len(rules), "rules": rules}


def write_cta_rules(rds, env_name: str, rules: Any) -> Dict[str, Any]:
    """校验并写入 {env}:cta_rules。errors 非空时抛 ValueError。"""
    key = cta_rules_redis_key(env_name)
    errors = sync_cta_rules.validate_rules(rules)
    if errors:
        raise ValueError("cta rules 校验失败: " + "; ".join(errors))
    payload = json.dumps(rules, ensure_ascii=False, separators=(",", ":"))
    rds.set(key, payload)
    return {"key": key, "count": len(rules), "bytes": len(payload)}


def render_cta_rules_panel_html() -> str:
    return """
    <section id="cta-rules" class="panel" style="display:none">
      <div class="section-header">
        <h2>CTA Rules <span class="badge" id="cta-rules-key"></span></h2>
        <div class="actions">
          <button id="cta-rules-load" class="secondary">读取</button>
          <button id="cta-rules-add" class="secondary">新增规则</button>
          <button id="cta-rules-check" class="ghost">校验</button>
          <button id="cta-rules-save">保存</button>
        </div>
      </div>
      <div class="hint">
        `{env}:cta_rules` JSON 数组，trade_signal 每 60s 热加载。每行一条独立规则；
        enabled 关闭即下线该规则。校验与 Rust loader（deny_unknown_fields）一致。
      </div>
      <div id="cta-rules-list"></div>
      <div id="cta-rules-status" class="status"></div>
    </section>
"""


def render_cta_rules_panel_js() -> str:
    fields_json = json.dumps(RULE_FIELDS, ensure_ascii=False)
    groups_json = json.dumps(GROUP_LABELS, ensure_ascii=False)
    defaults_json = json.dumps(sync_cta_rules.RULE_DEFAULTS, ensure_ascii=False)
    return (
        """
    const CTA_RULE_FIELDS = """
        + fields_json
        + """;
    const CTA_RULE_GROUPS = """
        + groups_json
        + """;
    const CTA_RULE_DEFAULTS = """
        + defaults_json
        + """;

    function ctaRulesSection() { return document.getElementById('cta-rules'); }

    // 相对当前页面路径取 API（nginx 挂在 /cta/<env>/config 前缀下，不能用 /api 绝对路径）
    function ctaApiUrl() {
      const base = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';
      return `${base}api/cta-rules`;
    }

    function ctaEsc(v) {
      return String(v ?? '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function ctaFieldHtml(field, value) {
      if (field.type === 'bool') {
        return `<label class="inline"><input type="checkbox" data-field="${field.name}" ${value ? 'checked' : ''}/> ${field.label}</label>`;
      }
      if (field.type === 'select') {
        const opts = (field.options || []).map(o => `<option value="${o}" ${o === value ? 'selected' : ''}>${o}</option>`).join('');
        return `<label>${field.label}</label><select data-field="${field.name}">${opts}</select>`;
      }
      if (field.type === 'offsets') {
        const text = Array.isArray(value) ? value.join(', ') : (value ?? '');
        return `<label>${field.label}</label><input data-field="${field.name}" class="mono" value="${ctaEsc(text)}"/>`;
      }
      const step = field.step ? ` step="${field.step}"` : '';
      const num = field.type === 'int' || field.type === 'number' ? ' type="number"' : '';
      const v = (value === undefined || value === null) ? '' : value;
      return `<label>${field.label}</label><input${num}${step} data-field="${field.name}" class="mono" value="${ctaEsc(v)}"/>`;
    }

    function ctaRuleCard(rule, index) {
      const card = document.createElement('div');
      card.className = 'panel cta-rule-card';
      card.style.marginBottom = '12px';
      let html = `<div class="section-header"><h3><span class="cta-idx">#${index + 1}</span> <span class="mono">${ctaEsc(rule.rule_id) || '(new)'}</span></h3>
        <div class="actions">
          <button class="ghost cta-dup">复制</button>
          <button class="ghost cta-del">删除</button>
        </div></div>`;
      for (const group of Object.keys(CTA_RULE_GROUPS)) {
        html += `<h4 style="margin:10px 0 6px;color:var(--muted)">${CTA_RULE_GROUPS[group]}</h4><div class="grid-2">`;
        for (const field of CTA_RULE_FIELDS.filter(f => f.group === group)) {
          html += `<div class="field" title="${field.hint || ''}">${ctaFieldHtml(field, rule[field.name])}</div>`;
        }
        html += '</div>';
      }
      card.innerHTML = html;
      card.querySelector('.cta-del').addEventListener('click', () => { card.remove(); ctaReindex(); });
      card.querySelector('.cta-dup').addEventListener('click', () => {
        const rules = ctaCollectRules();
        const copy = {...rules[index]};
        copy.rule_id = (copy.rule_id || 'rule') + '_copy';
        rules.splice(index + 1, 0, copy);
        renderCtaRules(rules);
      });
      return card;
    }

    function ctaCollectCard(card) {
      const rule = {};
      for (const field of CTA_RULE_FIELDS) {
        const el = card.querySelector(`[data-field="${field.name}"]`);
        if (!el) continue;
        if (field.type === 'bool') { rule[field.name] = el.checked; continue; }
        const raw = (el.value || '').trim();
        if (field.type === 'offsets') {
          rule[field.name] = raw ? raw.split(/[\\s,]+/).map(Number).filter(v => !Number.isNaN(v)) : [];
        } else if (field.type === 'int') {
          rule[field.name] = raw === '' ? CTA_RULE_DEFAULTS[field.name] : parseInt(raw, 10);
        } else if (field.type === 'number') {
          rule[field.name] = raw === '' ? CTA_RULE_DEFAULTS[field.name] : Number(raw);
        } else {
          rule[field.name] = raw;
        }
      }
      return rule;
    }

    function ctaCollectRules() {
      return [...document.querySelectorAll('#cta-rules-list .cta-rule-card')].map(ctaCollectCard);
    }

    function ctaReindex() {
      document.querySelectorAll('#cta-rules-list .cta-rule-card .cta-idx').forEach((el, i) => {
        el.textContent = `#${i + 1}`;
      });
    }

    function renderCtaRules(rules) {
      const list = document.getElementById('cta-rules-list');
      list.innerHTML = '';
      (rules || []).forEach((rule, i) => {
        list.appendChild(ctaRuleCard({...CTA_RULE_DEFAULTS, ...rule}, i));
      });
      if (!(rules || []).length) {
        list.innerHTML = '<div class="hint">（空 — 点击「新增规则」创建第一条）</div>';
      }
    }

    async function ctaRulesLoad() {
      const data = await fetch(ctaApiUrl()).then(r => r.json());
      if (data.error) { setStatus('cta-rules-status', data.error, false); return; }
      document.getElementById('cta-rules-key').textContent = data.key || '';
      renderCtaRules(data.rules || []);
      setStatus('cta-rules-status', `Loaded ${data.count} rules${data.exists ? '' : ' (key 不存在)'}` + (data.parse_error ? ` — parse_error: ${data.parse_error}` : ''), !data.parse_error);
    }

    async function ctaRulesSave(dryRun) {
      const rules = ctaCollectRules();
      const resp = await fetch(ctaApiUrl(), {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({rules, dry_run: !!dryRun}),
      }).then(r => r.json());
      if (resp.error || resp.errors) {
        const msg = resp.error || (resp.errors || []).join('; ');
        setStatus('cta-rules-status', msg, false);
        return;
      }
      setStatus('cta-rules-status', `${dryRun ? '校验通过' : 'Saved'} ${resp.count} rules -> ${resp.key}`, true);
    }

    function bindCtaRulesPanel() {
      if (!ctaRulesSection()) return;
      if (!(BOOTSTRAP.features && BOOTSTRAP.features.cta_rules)) {
        return;
      }
      ctaRulesSection().style.display = '';
      const nav = document.getElementById('cta-rules-nav-link');
      if (nav) nav.style.display = '';
      document.getElementById('cta-rules-load').addEventListener('click', ctaRulesLoad);
      document.getElementById('cta-rules-save').addEventListener('click', () => ctaRulesSave(false));
      document.getElementById('cta-rules-check').addEventListener('click', () => ctaRulesSave(true));
      document.getElementById('cta-rules-add').addEventListener('click', () => {
        const rules = ctaCollectRules();
        rules.push({...CTA_RULE_DEFAULTS, rule_id: `rule_${rules.length + 1}`});
        renderCtaRules(rules);
      });
      ctaRulesLoad();
    }
    """
    )
