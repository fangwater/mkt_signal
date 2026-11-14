# LTP 资金划转前端

独立 React 18 / Ant Design 5 页面，部署路径为 `/delist/transfers/`。所有脚本和样式均在 `dist/` 本地打包，无 CDN、API 凭据或生产模拟数据。

```bash
cd web/ltp_transfer/frontend
npm ci
npm run build
```

构建输出 `dist/index.html`、`dist/app.js`、`dist/app.css`。HTML 使用相对资源路径；接口使用相对 `api/...`，认证失效跳转 `/delist/login`。

页面从 catalog 加载账户范围，从 routes 加载双方可用币种、网络与 `requires_network`；余额展示为可划转额度。预览不执行划转，确认弹窗展示服务端校验后的完整请求。金额与账户编号始终使用字符串。

确认提交前把 `preview_id` 保存到 sessionStorage，执行 POST 只发一次。`preview_id` 同时是服务端 `client_order_id`。pending / submitting / unknown 保持锁定，断网、超时或无确定结果的错误只允许 GET 查询。只有最终 success / failed 或相同 `preview_id` 的显式 `{submitted:false}` 拒绝会解除锁定。历史记录每 15 秒进行 GET 更新。

`frontend/smoke.mjs` 使用本地 HTTP 服务和完整模拟 API，不访问真实 LTP。通过现有 Playwright 安装运行：

```bash
PLAYWRIGHT_MODULE=/absolute/path/to/playwright/index.mjs node smoke.mjs
```

也可使用默认模块 `playwright`。可选 `CHROMIUM_PATH` 指定现有 Chromium，`SCREENSHOT_DIR` 指定已存在的截图目录。覆盖精度、预览无执行、双击单次提交、未完成锁定、断网刷新恢复、过期、明确拒绝、必选网络、历史查询警告、实际到账/费用/划转方式与偏好分离、中文账户类型、移动端布局、资源本地化与登录跳转。
