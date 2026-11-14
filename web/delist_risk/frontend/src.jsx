import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Alert, Card, ConfigProvider, Space, Table, Tag, Typography } from 'antd';

const { Text } = Typography;
const DAY = 86_400_000;

function yieldValue(value, covered, days, danger = false) {
  if (value == null) return <Text type="secondary">--</Text>;
  const percent = danger && value >= 0.09995 ? '<10.00%' : `${(value * 100).toFixed(2)}%`;
  return <Space direction="vertical" size={0}>
    <Text type={danger ? 'danger' : undefined} strong={danger}>{percent}</Text>
    {covered < days * DAY && <Text type="secondary">不足 {days} 天</Text>}
  </Space>;
}

const columns = [
  {
    title: '账户', dataIndex: 'slug', key: 'slug', ellipsis: true,
    render: (slug, account) => <Space direction="vertical" size={0}>
      <Text strong>{account.alias || slug}</Text>
      {account.alias && account.alias !== slug && <Text type="secondary">{slug}</Text>}
    </Space>,
  },
  { title: '近 7 日年化', key: '7d', width: 120, align: 'right', render: (_, a) => yieldValue(a.annualized_return, a.covered_ms, 7, true) },
  { title: '近 30 日年化', key: '30d', width: 120, align: 'right', render: (_, a) => yieldValue(a.annualized_return_30d, a.covered_30d_ms, 30) },
];

function YieldAlerts() {
  const [report, setReport] = useState(null);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const response = await fetch(new URL('fr-yield', location.href), { cache: 'no-store' });
        if (response.status === 401) { location.replace('/delist/login'); return; }
        if (!response.ok) throw new Error('request failed');
        const data = await response.json();
        if (active) { setReport(data); setFailed(false); }
      } catch {
        if (active) { setReport(null); setFailed(true); }
      }
    };
    load();
    const timer = window.setInterval(load, 60_000);
    const refresh = document.getElementById('refresh');
    refresh?.addEventListener('click', load);
    return () => { active = false; window.clearInterval(timer); refresh?.removeEventListener('click', load); };
  }, []);
  if (!report && !failed) return null;
  if (failed || !report?.catalog_current) return <Alert type="warning" showIcon message="FR 年化暂不可用" description="NAV 数据或 FR 名单不可用、过期，无法判断是否低于 10%。" />;
  const alerts = report.alerts || [];
  const unavailable = report.unavailable_count || 0;
  if (!alerts.length && !unavailable) return null;
  return <Space direction="vertical" size="middle" style={{ display: 'flex', marginTop: 20 }}>
    {alerts.length > 0 && <Card title={<Space>FR 收益提醒 <Tag color="error">近 7 日低于 10%</Tag></Space>} size="small">
      <Table columns={columns} dataSource={alerts} rowKey="slug" pagination={false} size="small" scroll={{ x: 420 }} />
    </Card>}
    {unavailable > 0 && <Alert type="warning" showIcon message={`${unavailable} 个 FR 年化数据不可用`} />}
  </Space>;
}

const root = document.getElementById('fr-yield');
if (root) createRoot(root).render(<ConfigProvider theme={{ token: { colorPrimary: '#30685d', borderRadius: 8, fontFamily: 'system-ui, sans-serif' } }}><YieldAlerts /></ConfigProvider>);
