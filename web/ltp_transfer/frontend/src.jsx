import React, { useCallback, useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Alert, Button, ConfigProvider, Empty, Form, Input, Modal, Select, Skeleton, Space, Switch, Table, Tag, Tooltip } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import 'antd/dist/reset.css';
import './style.css';

const LOCK_KEY = 'ltp-transfer-attempt';
const TERMINAL = new Set(['success', 'failed']);
const STATUS = { submitting: ['提交中', 'processing'], pending: ['处理中', 'processing'], success: ['已完成', 'success'], failed: ['失败', 'error'], unknown: ['结果未知', 'warning'] };
const ACCOUNT_TYPES = { U_MARGINED_FUTURES: '美元本位合约', C_MARGINED_FUTURES: '币本位合约', MARGIN_CROSS: '全仓杠杆', MARGIN_ISOLATED: '逐仓杠杆', SPOT: '现货', FUNDING: '资金钱包', TRADING: '交易钱包', UNIFIED: '统一账户', PORTFOLIO_MARGIN: '组合保证金' };
const KIND = { funding: '资金账户', dma: 'DMA 账户', portfolio: '子组合' };
const time = value => value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '—';
const decimal = value => { const [whole, fraction] = String(value ?? '—').split('.'); return whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + (fraction === undefined ? '' : `.${fraction}`); };
function Icon({ name, size = 20, ...props }) {
  const paths = { arrow: 'M5 12h14m-5-5 5 5-5 5', refresh: 'M20 7v5h-5M4 17v-5h5M6.1 7a7 7 0 0 1 11.6-1L20 9M4 15l2.3 3A7 7 0 0 0 18 17', shield: 'M12 3 4 6v6c0 5 8 9 8 9s8-4 8-9V6l-8-3Zm-4 9 3 3 5-6', wallet: 'M4 5h14v4M4 5a2 2 0 0 0 0 4h16v11H4a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2Zm16 8h-5v3h5', swap: 'M4 8h16m-4-4 4 4-4 4M20 16H4m4-4-4 4 4 4', layers: 'm12 3 9 5-9 5-9-5 9-5ZM3 12l9 5 9-5M3 16l9 5 9-5', clock: 'M12 8v5l3 2M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z' };
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}><path d={paths[name] || paths.arrow} /></svg>;
}
async function api(path, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);
  try {
    const response = await fetch(`api/${path}`, { credentials: 'same-origin', cache: 'no-store', ...options, signal: controller.signal, headers: { ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers } });
    if (response.status === 401) { window.location.assign('/delist/login'); throw new Error('登录已过期，正在跳转'); }
    const data = await response.json().catch(() => null);
    if (!response.ok) { const error = new Error(data?.error || `请求失败（${response.status}）`); error.submitted = data?.submitted; error.preview_id = data?.preview_id; throw error; }
    if (!data) throw new Error('服务返回了无法识别的响应');
    return data;
  } catch (error) { if (error.name === 'AbortError') throw new Error('请求超时，请查询操作状态'); throw error; }
  finally { clearTimeout(timeout); }
}
function initialLock() { try { return JSON.parse(sessionStorage.getItem(LOCK_KEY) || 'null'); } catch { return { preview_id: '', storage_error: true }; } }
function Status({ status }) { const [label, color] = STATUS[status] || ['结果未知', 'warning']; return <Tag color={color} className="status-tag"><span className={`status-dot ${color}`} />{label}</Tag>; }
function AccountTypeLabel({ value }) {
  const label = ACCOUNT_TYPES[value];
  return <span className="account-type-label" title={label ? `${label} · ${value}` : value}><span>{label || value || '请选择账户类型'}</span>{label && <code>{value}</code>}</span>;
}
function RecordDetails({ record }) {
  return <div className="record-detail">
    <span>操作编号 <b className="mono">{record.client_order_id}</b></span>
    <span>LTP 编号 <b className="mono">{record.transfer_id || '尚未返回'}</b></span>
    <span>最后更新 <b>{time(record.updated_at)}</b></span>
    <span>实际到账 <b className="mono">{record.amount_received != null ? `${decimal(record.amount_received)} ${record.currency}` : '未返回'}</b></span>
    <span>实际网络费用 <b className="mono">{record.network_fee != null ? `${decimal(record.network_fee)} ${record.fee_currency ?? '（费用币种未返回）'}` : '未返回'}</b></span>
    <span>实际划转方式 <b>{record.rapid_transfer === true ? '快速划转' : record.rapid_transfer === false ? '普通划转' : '未返回 / 待确认'}</b></span>
    <span>提交时的划转偏好 <b>{record.requested_rapid_transfer === true ? '优先快速划转' : record.requested_rapid_transfer === false ? '普通划转' : '未返回'}</b></span>
    {record.reason && <span>处理说明 <b>{record.reason}</b></span>}
  </div>;
}
function AccountLabel({ account }) { return <span className="account-option"><span>{account.name}</span><small>{account.venue || KIND[account.kind]}</small></span>; }
function BalancePanel({ label, account, accountType, currency, refreshKey, onBalance }) {
  const [state, setState] = useState({ loading: false, data: null, error: '' });
  useEffect(() => {
    let active = true;
    onBalance?.(null);
    if (!account?.id || !accountType) { setState({ loading: false, data: null, error: '' }); return; }
    setState({ loading: true, data: null, error: '' });
    api(`balances?${new URLSearchParams({ account_id: account.id, account_type: accountType })}`).then(data => { if (active) { setState({ loading: false, data, error: '' }); onBalance?.(data); } }).catch(error => { if (active) setState({ loading: false, data: null, error: error.message }); });
    return () => { active = false; };
  }, [account?.id, accountType, refreshKey, onBalance]);
  const rows = state.data?.balances || [];
  const selected = rows.filter(row => row.currency === currency);
  return <section className="balance-panel">
    <div className="balance-label"><span>{label}</span>{account && <Tag>{KIND[account.kind] || account.kind}</Tag>}</div>
    <h3>{account?.name || '选择账户查看余额'}</h3>
    <div className="account-detail">{account ? <>{account.venue || 'LTP'}<span>·</span><AccountTypeLabel value={accountType}/></> : '账户余额由 LTP 实时查询'}</div>
    {state.loading ? <Skeleton active paragraph={{ rows: 2 }} title={false} /> : state.error ? <Alert type="error" showIcon message="余额暂不可用" description={state.error} /> : !account ? <div className="balance-placeholder"><Icon name="wallet" size={28} /><span>先选择划转账户</span></div> : selected.length ? selected.map((row, i) => <div className="balance-asset" key={`${row.currency}-${row.network}-${i}`}><span className="muted">可划转余额{row.network ? ` · ${row.network}` : ''}</span><div className="balance-number">{decimal(row.available)}<span>{row.currency}</span></div><div className="balance-total">总余额 <span>{decimal(row.total)} {row.currency}</span></div></div>) : <div className="balance-placeholder"><span>{currency ? `暂无 ${currency} 余额数据` : '请选择币种'}</span></div>}
    {state.data && <div className="balance-update">更新于 {time(state.data.updated_at)}</div>}
  </section>;
}
function App() {
  const [form] = Form.useForm();
  const [catalog, setCatalog] = useState(null);
  const [catalogError, setCatalogError] = useState('');
  const [routes, setRoutes] = useState({ data: null, loading: false, error: '' });
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [history, setHistory] = useState({ records: [], updated_at: '' });
  const [historyLoading, setHistoryLoading] = useState(true);
  const [historyError, setHistoryError] = useState('');
  const [page, setPage] = useState(1);
  const [refreshKey, setRefreshKey] = useState(0);
  const [sourceBalance, setSourceBalance] = useState(null);
  const [preview, setPreview] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [submitLoading, setSubmitLoading] = useState(false);
  const [lock, setLock] = useState(initialLock);
  const [notice, setNotice] = useState(null);
  const [actionError, setActionError] = useState('');
  const [querying, setQuerying] = useState('');
  const [now, setNow] = useState(Date.now());
  const submitGuard = useRef(false);
  const historyGeneration = useRef(0);
  const lockRef = useRef(lock); lockRef.current = lock;
  const fromId = Form.useWatch('from_account_id', form);
  const toId = Form.useWatch('to_account_id', form);
  const fromType = Form.useWatch('from_account_type', form);
  const toType = Form.useWatch('to_account_type', form);
  const currency = Form.useWatch('currency', form);
  const accounts = catalog?.accounts || [];
  const source = accounts.find(account => account.id === fromId);
  const destination = accounts.find(account => account.id === toId);
  const fundingRoute = source?.kind === 'funding' || destination?.kind === 'funding';
  const rapidAllowed = currency === 'USDT' && source && destination && !fundingRoute;
  const networkRequired = Boolean(routes.data?.requires_network);
  const routeCurrencies = routes.data?.currencies || [];
  const networks = routeCurrencies.find(item => item.currency === currency)?.networks || [];
  const routeReady = Boolean(source && destination && fromType && toType && routes.data && !routes.loading && !routes.error);
  const busySource = history.records.some(record => !TERMINAL.has(record.status) && record.from_account_id === fromId);
  const blocked = Boolean(lock) || busySource;
  const previewExpired = preview && new Date(preview.expires_at).getTime() <= now;
  const clearLock = useCallback(() => { sessionStorage.removeItem(LOCK_KEY); setLock(null); }, []);
  const acceptRecord = useCallback(record => {
    setHistory(previous => ({ ...previous, records: [record, ...previous.records.filter(item => item.client_order_id !== record.client_order_id)] }));
    if (TERMINAL.has(record.status)) {
      if (lockRef.current?.preview_id === record.client_order_id) clearLock();
      setRefreshKey(key => key + 1);
    }
    return record;
  }, [clearLock]);
  const loadHistory = useCallback(async (visible = false) => {
    const generation = ++historyGeneration.current;
    if (visible) setHistoryLoading(true);
    try {
      const data = await api(`transfers?page=${page}&page_size=20`);
      if (generation !== historyGeneration.current) return;
      setHistory(data); setHistoryError('');
      const record = data.records.find(item => item.client_order_id === lockRef.current?.preview_id);
      if (record && TERMINAL.has(record.status)) { clearLock(); setRefreshKey(key => key + 1); }
    } catch (error) { if (generation === historyGeneration.current) setHistoryError(error.message); }
    finally { if (generation === historyGeneration.current) setHistoryLoading(false); }
  }, [page, clearLock]);
  const loadCatalog = useCallback(async () => {
    setCatalogLoading(true); setCatalogError('');
    try { const data = await api('catalog'); setCatalog(data); if (!form.getFieldValue('currency')) form.setFieldValue('currency', data.currencies.find(item => item.currency === 'USDT')?.currency || data.currencies[0]?.currency); }
    catch (error) { setCatalogError(error.message); }
    finally { setCatalogLoading(false); }
  }, [form]);
  useEffect(() => { loadCatalog(); }, [loadCatalog]);
  useEffect(() => { loadHistory(true); const interval = setInterval(() => loadHistory(false), 15000); return () => clearInterval(interval); }, [loadHistory]);
  useEffect(() => { if (!preview) return; const interval = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(interval); }, [preview]);
  useEffect(() => { if (!rapidAllowed) form.setFieldValue('rapid_transfer', false); }, [rapidAllowed, form]);
  useEffect(() => {
    let active = true;
    form.setFieldValue('network', undefined);
    if (!fromId || !toId || !fromType || !toType || !currency) { setRoutes({ data: null, loading: false, error: '' }); return; }
    setRoutes({ data: null, loading: true, error: '' });
    api(`routes?${new URLSearchParams({ from_account_id: fromId, to_account_id: toId, from_account_type: fromType, to_account_type: toType, currency })}`).then(data => {
      if (!active) return;
      setRoutes({ data, loading: false, error: '' });
      if (!data.currencies.some(item => item.currency === currency) && data.currencies.length) form.setFieldValue('currency', data.currencies[0].currency);
    }).catch(error => { if (active) setRoutes({ data: null, loading: false, error: error.message }); });
    return () => { active = false; };
  }, [fromId, toId, fromType, toType, currency, form, refreshKey]);
  const query = async id => {
    if (!id || querying) return;
    setQuerying(id); setActionError('');
    try { const record = acceptRecord(await api(`transfers/${encodeURIComponent(id)}`)); if (TERMINAL.has(record.status)) { setPreview(null); setNotice({ type: record.status === 'success' ? 'success' : 'error', message: record.status === 'success' ? '划转已完成' : '划转失败', description: record.reason || `操作编号：${record.client_order_id}` }); } }
    catch (error) { setActionError(`查询未完成：${error.message}。原请求已保留，请继续查询，不要重复提交。`); }
    finally { setQuerying(''); }
  };
  const makePreview = async values => {
    if (blocked || previewLoading || !routeReady) return;
    setPreviewLoading(true); setActionError(''); setNotice(null);
    try {
      const request = { ...values, amount: values.amount.trim(), rapid_transfer: Boolean(values.rapid_transfer) };
      if (!request.network) delete request.network;
      const data = await api('transfers/preview', { method: 'POST', body: JSON.stringify(request) });
      setNow(Date.now()); setPreview(data); submitGuard.current = false;
    } catch (error) { setActionError(error.message); }
    finally { setPreviewLoading(false); }
  };
  const submit = async () => {
    if (!preview || previewExpired || submitGuard.current || lock) return;
    const attempt = { preview_id: preview.preview_id, created_at: new Date().toISOString() };
    try { sessionStorage.setItem(LOCK_KEY, JSON.stringify(attempt)); }
    catch { setActionError('浏览器无法保留操作记录，请允许本站会话存储后再提交。'); return; }
    submitGuard.current = true; setLock(attempt); setSubmitLoading(true); setActionError('');
    try {
      const record = acceptRecord(await api('transfers', { method: 'POST', body: JSON.stringify({ preview_id: preview.preview_id }) }));
      setPreview(null);
      setNotice({ type: record.status === 'success' ? 'success' : record.status === 'failed' ? 'error' : 'info', message: record.status === 'success' ? '划转已完成' : record.status === 'failed' ? '划转失败' : '请求已受理，等待确认结果', description: record.reason || `操作编号：${record.client_order_id}` });
      if (TERMINAL.has(record.status)) clearLock();
    } catch (error) {
      setPreview(null);
      if (error.submitted === false && error.preview_id === attempt.preview_id) {
        clearLock();
        setActionError(`划转未提交：${error.message}。请刷新信息后重新预览。`);
        setRefreshKey(key => key + 1);
      } else {
        setActionError(`提交结果尚未确认：${error.message}。请查询原操作；页面不会自动重试提交。`);
        // GET only: the preview ID is also the durable operation ID on the server.
        try { acceptRecord(await api(`transfers/${encodeURIComponent(attempt.preview_id)}`)); } catch { /* Keep the original attempt locked until a definitive result is known. */ }
      }
    } finally { setSubmitLoading(false); loadHistory(); }
  };
  const accountName = id => accounts.find(account => account.id === id)?.name || id;
  const columns = [
    { title: '创建时间 / 操作编号', key: 'created', width: 220, render: (_, record) => <div className="table-primary">{time(record.created_at)}<Tooltip title={record.client_order_id}><span className="table-secondary mono truncate">{record.client_order_id}</span></Tooltip></div> },
    { title: '划转路径', key: 'route', width: 330, render: (_, record) => <div className="history-route"><div><strong>{accountName(record.from_account_id)}</strong><small><AccountTypeLabel value={record.from_account_type}/></small></div><Icon name="arrow" size={17}/><div><strong>{accountName(record.to_account_id)}</strong><small><AccountTypeLabel value={record.to_account_type}/></small></div></div> },
    { title: '数量', key: 'amount', width: 190, align: 'right', render: (_, record) => <div className="table-primary mono">{decimal(record.amount)}<span className="table-secondary">{record.currency}</span></div> },
    { title: '状态', key: 'status', width: 125, render: (_, record) => <Tooltip title={record.reason}><span><Status status={record.status}/></span></Tooltip> },
    { title: '操作人', dataIndex: 'actor', width: 120, render: value => <span className="muted">{value || '—'}</span> },
    { title: '', key: 'action', width: 90, fixed: 'right', render: (_, record) => !TERMINAL.has(record.status) ? <Button type="link" size="small" loading={querying === record.client_order_id} disabled={Boolean(querying) && querying !== record.client_order_id} onClick={() => query(record.client_order_id)}>查询状态</Button> : <span className="muted">—</span> },
  ];
  const availableRows = sourceBalance?.balances.filter(item => item.currency === currency) || [];
  return <ConfigProvider locale={zhCN} theme={{ token: { colorPrimary: '#087f63', colorInfo: '#087f63', colorSuccess: '#087f63', colorText: '#20362f', colorTextSecondary: '#728078', colorBorder: '#dfe7e2', borderRadius: 9, fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif', controlHeight: 42 }, components: { Button: { primaryShadow: '0 3px 8px rgba(8,127,99,.13)' }, Table: { headerBg: '#f7f9f8', headerColor: '#748078' }, Modal: { borderRadiusLG: 18 } } }}>
    <header className="topbar"><div className="topbar-inner"><a href="/delist/" className="brand"><span className="brand-mark"><Icon name="layers" size={23}/></span><span>Risk Monitor<small>资产与风险工作台</small></span></a><nav aria-label="主导航"><a href="/delist/">FR 风险</a><a href="/delist/?view=cta">CTA 风险</a><a href="/delist/transfers/" className="active" aria-current="page">资金划转</a></nav><span className="session-pill"><span/>已登录会话</span></div></header>
    <main className="page-shell">
      <div className="page-heading"><div><div className="eyebrow">TREASURY / LTP</div><h1>资金划转<span className="title-badge">LTP</span></h1><p>在资金账户、DMA 与子组合之间调配资产。</p></div><Button icon={<Icon name="refresh" size={17}/>} onClick={() => { loadCatalog(); loadHistory(true); setRefreshKey(key => key + 1); }} loading={catalogLoading}>刷新数据</Button></div>
      <div className="summary-strip"><div className="summary-item"><span className="summary-icon"><Icon name="layers"/></span><div><span className="summary-label">可操作账户</span><strong>{catalogLoading ? '—' : accounts.length}<small> 个账户</small></strong></div></div><div className="summary-item"><span className="summary-icon"><Icon name="wallet"/></span><div><span className="summary-label">支持资产</span><strong>{catalogLoading ? '—' : catalog?.currencies.length ?? '—'}<small> 种币种</small></strong></div></div><div className="summary-item summary-note"><Icon name="shield" size={21}/><div><strong>先预览，再确认</strong><span>预览仅校验信息，确认后才执行划转</span></div></div></div>
      {catalogError && <Alert className="page-alert" type="error" showIcon message="账户目录加载失败" description={catalogError} action={<Button onClick={loadCatalog}>重试</Button>}/>}
      {catalog?.warnings?.map((warning, index) => <Alert className="page-alert" key={index} type="warning" showIcon message={warning}/>)}
      {notice && <Alert className="page-alert" showIcon closable onClose={() => setNotice(null)} {...notice}/>}
      {actionError && <Alert className="page-alert" type="error" showIcon message={actionError}/>}
      {lock && <Alert className="page-alert" type="warning" showIcon message="原操作尚未确认，请先查询结果" description={lock.storage_error ? '无法读取浏览器保存的操作记录。请检查浏览器会话存储，并核实历史记录后继续。' : <span>已保留操作编号 <span className="mono">{lock.preview_id}</span>。处理中与结果未知均不代表划转完成。</span>} action={!lock.storage_error && <Button loading={querying === lock.preview_id || submitLoading} onClick={() => query(lock.preview_id)}>查询原操作</Button>}/>}
      <div className="workspace-grid">
        <section className="surface transfer-card"><div className="section-heading"><div className="section-icon"><Icon name="swap"/></div><div><h2>发起划转</h2><p>选择账户与资产，核对后提交</p></div><span className="step-chip">01 / 02</span></div>
          {catalogLoading && !catalog ? <div className="form-skeleton"><Skeleton active paragraph={{ rows: 10 }}/></div> : !catalog ? <Empty description="账户目录暂不可用"/> : !accounts.length ? <Empty description="当前没有可操作的 LTP 账户"/> : <Form form={form} layout="vertical" requiredMark={false} onFinish={makePreview} initialValues={{ rapid_transfer: false }} disabled={Boolean(lock) || submitLoading}>
            <div className="transfer-direction"><span className="direction-dot from"/><span>转出账户</span></div>
            <div className="account-fields"><Form.Item name="from_account_id" label="账户" rules={[{ required: true, message: '请选择转出账户' }]}><Select aria-label="转出账户" placeholder="选择转出账户" showSearch optionFilterProp="search" onChange={id => form.setFieldValue('from_account_type', accounts.find(account => account.id === id)?.account_types[0])} options={accounts.map(account => ({ value: account.id, label: <AccountLabel account={account}/>, search: `${account.name} ${account.id} ${account.venue}` }))}/></Form.Item><Form.Item name="from_account_type" label="账户类型" rules={[{ required: true, message: '请选择账户类型' }]}><Select aria-label="转出账户类型" placeholder="账户类型" options={(source?.account_types || []).map(value => ({ value, label: <AccountTypeLabel value={value}/> }))}/></Form.Item></div>
            <div className="direction-divider"><Icon name="arrow" size={17}/></div>
            <div className="transfer-direction"><span className="direction-dot to"/><span>转入账户</span></div>
            <div className="account-fields"><Form.Item name="to_account_id" label="账户" rules={[{ required: true, message: '请选择转入账户' }]}><Select aria-label="转入账户" placeholder="选择转入账户" showSearch optionFilterProp="search" onChange={id => form.setFieldValue('to_account_type', accounts.find(account => account.id === id)?.account_types[0])} options={accounts.map(account => ({ value: account.id, label: <AccountLabel account={account}/>, search: `${account.name} ${account.id} ${account.venue}` }))}/></Form.Item><Form.Item name="to_account_type" label="账户类型" dependencies={['from_account_id', 'to_account_id', 'from_account_type']} rules={[{ required: true, message: '请选择账户类型' }, { validator: (_, value) => fromId && fromId === toId && fromType === value ? Promise.reject(new Error('转入与转出账户不能相同')) : Promise.resolve() }]}><Select aria-label="转入账户类型" placeholder="账户类型" options={(destination?.account_types || []).map(value => ({ value, label: <AccountTypeLabel value={value}/> }))}/></Form.Item></div>
            <div className="asset-fields"><Form.Item name="currency" label="币种" rules={[{ required: true, message: '请选择币种' }]}><Select aria-label="币种" showSearch loading={routes.loading} options={(routeCurrencies.length ? routeCurrencies : catalog.currencies).map(item => ({ value: item.currency, label: item.currency }))}/></Form.Item><Form.Item name="amount" label="划转数量" rules={[{ required: true, message: '请输入数量' }, { validator: (_, value) => !value || (/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value.trim()) && /[1-9]/.test(value)) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 的十进制数量')) }]}><Input aria-label="划转数量" inputMode="decimal" autoComplete="off" placeholder="0.00" suffix={<span className="muted">{currency}</span>}/></Form.Item></div>
            {availableRows.length === 1 && <div className="available-line">转出可划转 <span className="mono">{decimal(availableRows[0].available)} {currency}</span></div>}
            {(networkRequired || networks.length > 0) && <Form.Item name="network" label="划转网络" rules={[{ required: networkRequired, message: '请选择双方支持的划转网络' }]} extra={fundingRoute ? "涉及资金账户的划转可能产生链上费用，以 LTP 实际处理结果为准。" : "跨交易所划转请选择双方支持的网络；快速划转不适用时将使用普通方式。"}><Select aria-label="划转网络" allowClear loading={routes.loading} placeholder={networkRequired ? "选择划转网络" : "默认网络 / 不适用"} options={networks.map(value => ({ value, label: value }))}/></Form.Item>}
            {routes.error && <Alert className="form-notice" type="error" showIcon message={`可划转路径查询失败：${routes.error}`}/>}
            {routes.data?.warnings?.map((warning, index) => <Alert className="form-notice" key={index} type="warning" showIcon message={warning}/>)}
            {routes.data && !routeCurrencies.length && <Alert className="form-notice" type="warning" showIcon message="所选账户间没有支持的划转币种。"/>}
            <div className="rapid-option"><div><strong>优先快速划转 <Tag>USDT</Tag></strong><p>仅限交易账户间 USDT 划转；不满足条件时按普通方式处理。</p></div><Form.Item name="rapid_transfer" valuePropName="checked" noStyle><Switch aria-label="优先快速划转" disabled={Boolean(lock) || !rapidAllowed}/></Form.Item></div>
            {busySource && !lock && <Alert className="form-notice" type="warning" showIcon message="该转出账户有未完成的操作，请先查询历史记录。"/>}
            <Button className="preview-button" type="primary" htmlType="submit" block loading={previewLoading} disabled={blocked || !catalog || !routeReady || !routeCurrencies.length || historyLoading || Boolean(historyError)} icon={<Icon name="arrow" size={18}/>}>预览划转</Button><div className="submit-caption"><Icon name="shield" size={14}/>此步骤不会移动资金</div>
          </Form>}
        </section>
        <aside className="balance-column"><section className="surface balances-card"><div className="section-heading compact"><div><div className="eyebrow">ACCOUNT BALANCES</div><h2>账户余额</h2></div><Tooltip title="重新查询所选账户余额"><Button type="text" aria-label="刷新余额" icon={<Icon name="refresh" size={18}/>} onClick={() => setRefreshKey(key => key + 1)}/></Tooltip></div><BalancePanel label="转出" account={source} accountType={fromType} currency={currency} refreshKey={refreshKey} onBalance={setSourceBalance}/><div className="balance-separator"><span/><Icon name="arrow" size={17}/><span/></div><BalancePanel label="转入" account={destination} accountType={toType} currency={currency} refreshKey={refreshKey}/></section><section className="workflow-note"><span className="note-icon"><Icon name="clock" size={20}/></span><div><h3>以最终状态为准</h3><p>请求受理后可能仍在处理。历史记录每 15 秒更新，也可主动查询单笔操作。</p></div></section></aside>
      </div>
      <section className="surface history-card"><div className="history-heading"><div><h2>划转记录<span className="live-indicator">15s 自动更新</span></h2><p>{history.updated_at ? `最近更新 ${time(history.updated_at)}` : '查看操作进度与最终结果'}</p></div><Button icon={<Icon name="refresh" size={16}/>} loading={historyLoading} onClick={() => loadHistory(true)}>刷新记录</Button></div>{historyError && <Alert className="history-error" type="error" showIcon message={`记录更新失败：${historyError}`}/>}{history.warnings?.map((warning, index) => <Alert className="history-warning" type="warning" showIcon key={index} message={warning}/>)}<Table rowKey="client_order_id" columns={columns} dataSource={history.records} loading={historyLoading} pagination={false} scroll={{ x: 1075 }} locale={{ emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无划转记录"/> }} expandable={{ expandedRowRender: record => <RecordDetails record={record}/> }}/><div className="history-footer"><span>仅展示已提交的操作；预览不计入记录。</span><Space><Button size="small" disabled={page === 1 || historyLoading} onClick={() => setPage(value => value - 1)}>上一页</Button><span>第 {page} 页</span><Button size="small" disabled={history.records.length < 20 || historyLoading} onClick={() => setPage(value => value + 1)}>下一页</Button></Space></div></section>
      <footer className="page-footer"><span>Risk Monitor <span> / </span> LTP Treasury</span><span>账户权限与可划转范围以服务端校验为准</span></footer>
    </main>
    <Modal title={null} open={Boolean(preview)} width={580} onCancel={() => !submitLoading && setPreview(null)} closable={!submitLoading} maskClosable={false} keyboard={!submitLoading} footer={preview && <div className="review-footer"><Button onClick={() => setPreview(null)} disabled={submitLoading}>返回修改</Button><Button type="primary" onClick={submit} loading={submitLoading} disabled={previewExpired || Boolean(lock)}>确认执行划转</Button></div>}>
      {preview && <div className="review-content"><span className="review-icon"><Icon name="shield" size={25}/></span><div className="eyebrow">REVIEW TRANSFER · 02 / 02</div><h2>确认这笔划转</h2><p className="muted">请核对以下信息。确认后将向 LTP 提交真实划转。</p><div className="review-amount">{decimal(preview.request.amount)}<span>{preview.request.currency}</span></div><div className="review-route"><div><small>转出</small><strong>{preview.from_name}</strong><AccountTypeLabel value={preview.request.from_account_type}/><code>{preview.request.from_account_id}</code></div><Icon name="arrow"/><div><small>转入</small><strong>{preview.to_name}</strong><AccountTypeLabel value={preview.request.to_account_type}/><code>{preview.request.to_account_id}</code></div></div><dl className="review-details"><div><dt>划转网络</dt><dd>{preview.request.network || '服务端默认 / 不适用'}</dd></div><div><dt>快速划转偏好</dt><dd>{preview.request.rapid_transfer ? '优先快速，不满足时普通划转' : '普通划转'}</dd></div><div><dt>转出可划转</dt><dd className="mono">{decimal(preview.available)} {preview.request.currency}</dd></div><div><dt>预览有效期至</dt><dd>{time(preview.expires_at)}</dd></div></dl>{preview.warnings?.map((warning, index) => <Alert className="review-warning" key={index} type="warning" showIcon message={warning}/>)}{previewExpired && <Alert type="warning" showIcon message="预览已过期，请返回重新预览。"/>}</div>}
    </Modal>
  </ConfigProvider>;
}

createRoot(document.getElementById('root')).render(<App/>);
