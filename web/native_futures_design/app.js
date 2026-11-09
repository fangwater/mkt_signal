/* Standalone architecture proposal. All order events are deterministic examples. */
"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const profiles = {
    ctp: {
      name: "CTP",
      venues: [["CFFEX", "CFFEX · 中金所"]],
      symbol: () => "IF",
      method: "ReqOrderInsert / ReqOrderAction",
      gateway: "CTP Trader API",
      description: "中金所 · 原生报单 / 撤单",
      kicker: "CTP NATIVE API",
      ingress: "CTP 交易回报",
      boundary:
        "CTP SDK 通常包含内部网络与回调线程。“一个 loop”指业务状态归属，不代表整个进程只有一个线程。",
      submit: "ReqOrderInsert",
      cancel: "ReqOrderAction",
      order: "OnRtnOrder",
      trade: "OnRtnTrade",
      query: "CTP 原生查询 / 回报对账",
    },
    yd: {
      name: "易达",
      venues: [
        ["SHFE", "SHFE · 上期所"],
        ["INE", "INE · 能源中心"],
      ],
      symbol: (venue) => (venue === "INE" ? "SC" : "CU"),
      method: "易达原生报单 / 撤单",
      gateway: "易达原生交易 SDK",
      description: "上期所 / 能源中心 · 原生交易",
      kicker: "YIDA NATIVE API",
      ingress: "易达原生交易事件",
      boundary:
        "此图只描述逻辑边界。易达 SDK 的回调 / 轮询方式、线程模型、调用约束与恢复顺序，须依据授权 SDK 核实；本页不假设与 CTP 相同。",
      submit: "易达原生报单 · 函数名待 SDK 核实",
      cancel: "易达原生撤单 · 函数名待 SDK 核实",
      order: "易达原生委托事件 · 字段待核实",
      trade: "易达原生成交事件 · 字段待核实",
      query: "易达原生恢复 / 查询 · 能力待核实",
    },
  };

  const nodeDetails = {
    ingress: {
      index: "IN",
      label: "NATIVE EVENT BOUNDARY",
      title: "接入事实事件，不把 SDK 线程当业务线程。",
      description:
        "原生适配器将委托、成交、拒单与查询结果映射成同一套核心事件，并保留原始身份和错误。回调数据在有效期内拷贝；入口队列类型按实际生产者线程数选择。",
      rules: [
        "核实生产者数量后选 SPSC / MPSC",
        "同源顺序保留，跨源不假设全序",
        "交易事件溢出使状态进入待恢复",
      ],
    },
    strategy: {
      index: "01",
      label: "TRADING INTENT",
      title: "策略只产生完整、可检查的交易意图。",
      description:
        "行情来源独立，暂按 CTP 考虑。轻量策略在业务 loop 中产生带合约、方向、开平、手数、价格 tick 与有效方式的意图；复杂模型能否同核执行以测量结果决定。",
      rules: [
        "不在策略层构造柜台结构体",
        "交易意图必须限定账户与交易所",
        "行情是否陈旧由本地接收时间检查",
      ],
    },
    risk: {
      index: "02",
      label: "PRE-TRADE CHECKS",
      title: "先校验与预占，再进入原生报单。",
      description:
        "用本核内存状态检查可用持仓、开仓额度、价格与数量规则、自成交和账户报撤单预算。通过后先登记待报委托并预占；本地失败或最终拒单在状态机中幂等释放。",
      rules: [
        "活跃合约与权限快照完整后才可交易",
        "风控计算包含全部未终结委托",
        "连接或恢复状态不完整时暂停新开仓",
      ],
    },
    orders: {
      index: "03",
      label: "STATE OWNERSHIP",
      title: "委托与仓位，只有一个写入者。",
      description:
        "同一个业务 loop 持有委托、今昨仓、冻结量、会话与风险状态。成交事实按原始身份去重；上期所与 INE 的平今、平昨子单按各自仓位日期预占和核销。",
      rules: [
        "成交才改变已成持仓",
        "撤单请求不立即释放冻结量",
        "重复与迟到回报不能重复记账",
      ],
    },
    submit: {
      index: "04",
      label: "DIRECT NATIVE CALL",
      title: "核心模块直接调用，适配器负责原生映射。",
      description:
        "策略、风控与执行之间使用函数调用。适配器将已批准的交易意图映射到当前柜台 SDK，维护本地订单与原生订单标识的关联，并分别处理本地返回和后续异步确认。",
      rules: [
        "一个实例显式绑定一个交易后端",
        "共享账户的流控集中预算",
        "API 返回成功不代表柜台确认或成交",
      ],
    },
    adapter: {
      index: "API",
      label: "NATIVE ADAPTER",
      title: "CTP 与易达各自原生实现。",
      description:
        "CTP 使用 Trader API；易达使用其授权原生 SDK。核心只有一套交易契约，各适配器保留真实指令语义、错误信息和恢复规则；不通过 CTP 兼容接口转接易达。",
      rules: [
        "资金计算兼容不等于 API / ABI 兼容",
        "易达函数与回调字段待 SDK 核实",
        "后端错误不触发跨柜台自动重投",
      ],
    },
    background: {
      index: "BG",
      label: "BACKGROUND I/O",
      title: "把阻塞工作移出执行路径。",
      description:
        "日志、状态展示、配置读取和落盘通过后台任务完成；配置以完整快照交给业务 loop 应用。异步记录本身不保证掉电可靠，下单前日志与刷盘策略需要单独确定。",
      rules: [
        "订单审计和恢复事件有序记录",
        "可靠事件队列满时暂停新开并恢复",
        "监控丢样与交易事件丢失分别处理",
      ],
    },
  };

  let backend = "ctp";
  let selectedNode = "orders";
  let cursor = 0;
  let steps = [];
  let state;
  let timer = null;
  let toastTimer;
  let modelApplied = false;

  function profile() {
    return profiles[backend];
  }
  function venue() {
    return $("venue").value;
  }
  function el(tag, className, text) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function selectNode(key) {
    selectedNode = key;
    const detail = nodeDetails[key];
    $("node-index").textContent = detail.index;
    $("node-label").textContent = detail.label;
    $("node-title").textContent = detail.title;
    $("node-description").textContent = detail.description;
    $("node-rules").replaceChildren(
      ...detail.rules.map((rule) => el("span", "", rule)),
    );
    document.querySelectorAll("[data-node]").forEach((button) => {
      const selected = button.dataset.node === key;
      button.classList.toggle("is-selected", selected);
      button.setAttribute("aria-pressed", String(selected));
    });
  }

  function setBackend(next) {
    stop();
    backend = next;
    const p = profile();
    document.querySelectorAll("[data-backend]").forEach((button) => {
      const selected = button.dataset.backend === backend;
      button.classList.toggle("selected", selected);
      button.setAttribute("aria-pressed", String(selected));
    });
    $("venue").replaceChildren(
      ...p.venues.map(([value, label]) => {
        const option = el("option", "", label);
        option.value = value;
        return option;
      }),
    );
    $("ingress-name").textContent = p.ingress;
    $("native-method").textContent = p.method;
    $("gateway-name").textContent = p.gateway;
    $("gateway-description").textContent = p.description;
    $("adapter-kicker").textContent = p.kicker;
    $("sdk-boundary").textContent = p.boundary;
    if (backend === "ctp" && $("scenario").value === "close")
      $("scenario").value = "fill";
    selectNode(selectedNode);
    reset();
  }

  const step = (
    action,
    source,
    result,
    node,
    explanation,
    apply,
    tone = "",
  ) => ({ action, source, result, node, explanation, apply, tone });

  function commonOpenSteps() {
    const p = profile();
    return [
      step(
        "收到行情，生成买开意图",
        "独立行情入口 / Strategy",
        "买开 3 手",
        "strategy",
        "行情只触发交易意图，此时没有委托或冻结量。合约为示意，不代表真实可交易合约。",
        () => {
          state.order = "待风控检查";
        },
      ),
      step(
        "风控通过，登记待报委托",
        "Native Core / Risk + OMS",
        "预占 3 手",
        "risk",
        "先登记订单并预占开仓额度，后续意图已经能看到这 3 手未成委托。",
        () => {
          state.pending = 3;
          state.order = "待报";
        },
      ),
      step(
        "调用原生报单，API 本地受理",
        p.submit,
        "等待异步确认",
        "submit",
        "本地调用成功仅表示提交阶段通过，不能据此改变已成持仓或认定柜台已接单。",
        () => {
          state.submits += 1;
          state.order = "已提交，待确认";
        },
      ),
      step(
        "柜台委托回报确认",
        p.order,
        "工作中",
        "orders",
        "将原生委托身份关联到本地订单；持仓仍为 0，未成额度仍预占 3 手。",
        () => {
          state.order = "工作中";
        },
      ),
    ];
  }

  function buildSteps(scenario) {
    const p = profile();
    if (scenario === "risk")
      return [
        step(
          "策略请求买开 3 手",
          "Strategy / TradingIntent",
          "等待检查",
          "strategy",
          "本场景演示可用开仓额度仅有 2 手，策略意图请求 3 手。",
          () => {
            state.order = "待风控检查";
          },
        ),
        step(
          "开仓额度不足，拒绝意图",
          "Native Core / Pre-trade",
          "3 > 2 手",
          "risk",
          "检查失败时不登记可发送委托、不预占额度，也不调用任何柜台报单 API。",
          () => {
            state.order = "风控拒绝";
          },
          "failure",
        ),
      ];
    if (scenario === "close")
      return [
        step(
          "策略请求卖平 3 手",
          "Strategy / CloseIntent",
          "今 1 / 昨 2",
          "strategy",
          "示例初始多头今仓 1 手、昨仓 2 手，按平今优先的显式策略分配。真实费用与优先规则由账户配置确定。",
          () => {
            state.order = "待拆分";
          },
        ),
        step(
          "拆分平今与平昨子单",
          "Native Core / OffsetPlanner",
          "CT 1 + CY 2",
          "orders",
          "上期所 / INE 的仓位日期分别管理。冻结今仓 1 手、昨仓 2 手，防止其他意图重复使用。",
          () => {
            state.frozenToday = 1;
            state.frozenYesterday = 2;
            state.children = ["CloseToday × 1", "CloseYesterday × 2"];
            state.order = "2 个子单待报";
          },
        ),
        step(
          "分别调用易达原生报单",
          p.submit,
          "2 个子单",
          "submit",
          "核心拆单计划映射为易达原生指令；实际字段与调用名须依据 SDK 联调。两个子单不能假设原子成交。",
          () => {
            state.submits = 2;
            state.order = "子单待确认";
          },
        ),
        step(
          "平今子单成交 1 手",
          p.trade,
          "今仓归零",
          "orders",
          "核销对应平今冻结量并扣减今仓。平昨子单仍占用昨仓 2 手。",
          () => {
            state.today = 0;
            state.frozenToday = 0;
            state.order = "平今完成 / 平昨待成";
          },
        ),
        step(
          "平昨子单成交 2 手",
          p.trade,
          "全部平仓",
          "orders",
          "核销平昨冻结量并扣减昨仓。任一子单若被拒绝或撤销，独立释放其未成部分。",
          () => {
            state.yesterday = 0;
            state.frozenYesterday = 0;
            state.order = "两个子单已成";
          },
        ),
      ];
    const common = commonOpenSteps();
    if (scenario === "local_failure")
      return [
        ...common.slice(0, 2),
        step(
          "原生 API 返回明确本地失败",
          p.submit,
          "本地拒绝",
          "submit",
          "本例是 SDK 明确报告未受理的本地失败，幂等回滚 3 手预占。若发送结果不明，则保留占用并先查询，不能盲目重报。",
          () => {
            state.submits = 1;
            state.pending = 0;
            state.order = "本地失败";
          },
          "failure",
        ),
      ];
    if (scenario === "cancel")
      return [
        ...common,
        step(
          "成交回报：先成交 1 手",
          p.trade,
          "持仓 1 / 待成 2",
          "orders",
          "实际成交增加今仓 1 手，将未成预占由 3 降为 2。",
          () => {
            state.today = 1;
            state.pending = 2;
            state.order = "部分成交";
          },
        ),
        step(
          "提交剩余部分的撤单请求",
          p.cancel,
          "冻结保持 2 手",
          "submit",
          "发出撤单请求不代表撤单成功。余量继续占用额度，期间仍可能继续成交。",
          () => {
            state.order = "部分成交 / 撤单待确认";
          },
        ),
        step(
          "柜台确认剩余 2 手撤销",
          p.order,
          "释放余量 2 手",
          "orders",
          "确认终结后释放未成 2 手。实际已经成交的 1 手保留；取消与成交竞态仍按事实事件核销。",
          () => {
            state.pending = 0;
            state.order = "部分成交后撤销";
          },
        ),
      ];
    if (scenario === "reconnect")
      return [
        ...common,
        step(
          "交易连接断开",
          "Adapter / Session",
          "暂停新开仓",
          "ingress",
          "连接中断不清空本地委托或冻结量，未终结订单仍可能在交易所成交。",
          () => {
            state.session = "Disconnected";
            state.order = "断线，状态待核对";
          },
          "warning",
        ),
        step(
          "重新登录，进入恢复阶段",
          "Adapter / Recovery",
          "Recovering",
          "adapter",
          "恢复回报与查询通过业务 loop 合并。真实可用的回放能力、快照边界和结束条件须逐柜台验证。",
          () => {
            state.session = "Recovering";
          },
          "warning",
        ),
        step(
          "恢复证据：已成 1 手，余量 2 手",
          p.query,
          "持仓 / 委托核对",
          "orders",
          "本例以一笔已去重的真实成交证据核销 1 手，并结合账户与委托查询验证剩余占用。不能把快照当成可重复累加的成交。",
          () => {
            state.today = 1;
            state.pending = 2;
            state.order = "部分成交 / 工作中";
          },
          "warning",
        ),
        step(
          "恢复完成条件全部满足",
          "Native Core / RecoveryBarrier",
          "Ready",
          "risk",
          "仅在会话、订单、持仓及风险规则核对完整后解除新开仓阻断。已有订单仍保留 2 手未成预占。",
          () => {
            state.session = "Ready";
          },
        ),
      ];
    if (scenario === "overflow")
      return [
        ...common,
        step(
          "可靠交易事件入口发生溢出",
          "Adapter / ReliableEventIngress",
          "状态不可信",
          "ingress",
          "有界入口满时不能静默丢回报。适配器必须以独立可见的故障标记通知核心，保留占用并阻断新开仓。",
          () => {
            state.session = "NeedsReconcile";
            state.order = "未知，待对账";
          },
          "failure",
        ),
        step(
          "锁存故障，启动核对流程",
          "Native Core / Recovery",
          "等待完整恢复",
          "risk",
          "不以清空队列或重新连接代替恢复。恢复渠道不足时保持阻断；连接允许时仍应能处理必要撤单。",
          () => {},
          "warning",
        ),
      ];
    return [
      ...common,
      step(
        "成交 T-001：成交 1 手",
        p.trade,
        "持仓 1 / 待成 2",
        "orders",
        "记录成交的原始唯一身份，核销 1 手预占并增加今仓。示意身份 T-001 不代表任何柜台字段格式。",
        () => {
          state.seen.add("T-001");
          state.today = 1;
          state.pending = 2;
          state.order = "部分成交";
        },
      ),
      step(
        "再次收到成交 T-001",
        p.trade,
        "去重，账本不变",
        "orders",
        "相同成交身份再次出现时不重复增加持仓，不重复释放预占。身份键的作用域按实际柜台契约确定。",
        () => {},
        "warning",
      ),
      step(
        "成交 T-002：剩余 2 手成交",
        p.trade,
        "持仓 3 / 待成 0",
        "orders",
        "最终成交归并后今仓为 3 手，未成预占为 0。重复或迟到的委托回报不能把已成状态退回工作中。",
        () => {
          state.seen.add("T-002");
          state.today = 3;
          state.pending = 0;
          state.order = "全部成交";
        },
      ),
    ];
  }

  function renderState(explanation) {
    $("position-today").textContent = state.today;
    $("position-yesterday").textContent = state.yesterday;
    $("pending-open").textContent = state.pending;
    $("frozen-close").textContent = state.frozenToday + state.frozenYesterday;
    $("submit-count").textContent = state.submits;
    $("session-state").textContent = state.session;
    $("session-state").classList.toggle("warning", state.session !== "Ready");
    $("order-state").textContent = state.order;
    $("demo-explanation").textContent = explanation;
    $("step-count").textContent =
      `${String(cursor).padStart(2, "0")} / ${String(steps.length).padStart(2, "0")}`;
    $("demo-progress").style.width = `${(cursor / steps.length) * 100}%`;
    $("step").disabled = cursor >= steps.length;
    $("event-empty").hidden = cursor > 0;
    $("child-orders").hidden = state.children.length === 0;
    $("child-orders").replaceChildren(
      ...state.children.map((child) => el("span", "", child)),
    );
  }

  function reset() {
    stop();
    cursor = 0;
    const isClose = $("scenario").value === "close";
    state = {
      today: isClose ? 1 : 0,
      yesterday: isClose ? 2 : 0,
      pending: 0,
      frozenToday: 0,
      frozenYesterday: 0,
      submits: 0,
      session: "Ready",
      order: "尚无委托",
      children: [],
      seen: new Set(),
    };
    steps = buildSteps($("scenario").value);
    $("event-list").replaceChildren();
    const p = profile();
    const symbol = p.symbol(venue());
    $("demo-route").textContent = `${p.name} / ${venue()}`;
    $("demo-symbol").textContent = `${symbol} · 演示合约`;
    $("demo-intent").textContent =
      `${symbol} 演示合约 · ${isClose ? "卖平 3 手（今 1 / 昨 2）" : "买开 3 手"}`;
    $("play").textContent = "▶ 播放推演";
    renderState(
      "状态只由业务 loop 更新。演示步骤不表示真实时间或柜台性能。" +
        (backend === "yd"
          ? "易达事件为语义设计，实际接口待 SDK 验证。"
          : "本页未连接真实 CTP 柜台。"),
    );
    selectNode("orders");
  }

  function advance() {
    if (cursor >= steps.length) return;
    const current = steps[cursor];
    current.apply();
    cursor += 1;
    const row = el("li", "event-row");
    row.append(el("span", "event-sequence", String(cursor).padStart(3, "0")));
    const action = el("div", "event-action");
    action.append(
      el("strong", "", current.action),
      el("small", "", current.source),
    );
    row.append(
      action,
      el("span", `event-result ${current.tone}`, current.result),
    );
    $("event-list").append(row);
    $("event-list").scrollTop = $("event-list").scrollHeight;
    selectNode(current.node);
    renderState(current.explanation);
    if (cursor >= steps.length) {
      stop();
      $("play").textContent = "↻ 重新推演";
    }
  }

  function stop() {
    if (timer !== null) window.clearInterval(timer);
    timer = null;
    $("play").setAttribute("aria-pressed", "false");
    if (steps.length && cursor < steps.length)
      $("play").textContent = cursor ? "▶ 继续推演" : "▶ 播放推演";
  }

  function play() {
    if (timer !== null) {
      stop();
      return;
    }
    if (cursor >= steps.length) reset();
    advance();
    if (cursor < steps.length) {
      $("play").textContent = "Ⅱ 暂停推演";
      $("play").setAttribute("aria-pressed", "true");
      timer = window.setInterval(advance, 850);
    }
  }

  function modelValues() {
    return Object.fromEntries(
      ["business", "ipc", "schedule", "api"].map((key) => [
        key,
        Number($("cost-" + key).value),
      ]),
    );
  }

  function renderModel() {
    const values = modelValues();
    for (const [key, value] of Object.entries(values))
      $("value-" + key).textContent = value.toFixed(1) + " µs";
    if (!modelApplied) return;
    const handoff = 3 * (values.ipc + values.schedule);
    const direct = values.business + values.api;
    const split = direct + handoff;
    $("split-total").textContent = split.toFixed(1) + " µs";
    $("loop-total").textContent = direct.toFixed(1) + " µs";
    $("model-state").textContent = "手动假设 · 非实测";
    $("model-state").classList.add("blue-text");
    $("apply-model").textContent = "已套用 · 拖动即可更新";
    const drawBar = (id, parts, total) => {
      const bar = $(id);
      bar.classList.remove("placeholder-bar");
      bar.style.width = `${(total / split) * 100}%`;
      bar.replaceChildren(
        ...parts
          .filter(([, value]) => value > 0)
          .map(([name, value]) => {
            const segment = el("span", "cost-" + name);
            segment.style.flex = String(value);
            segment.title = `${name}: ${value.toFixed(1)} µs（假设）`;
            return segment;
          }),
      );
      bar.setAttribute("role", "img");
      bar.setAttribute(
        "aria-label",
        `${id === "split-bar" ? "参考分拆路径" : "单 loop 路径"}假设耗时 ${total.toFixed(1)} 微秒`,
      );
    };
    drawBar(
      "split-bar",
      [
        ["business", values.business],
        ["handoff", handoff],
        ["api", values.api],
      ],
      split,
    );
    drawBar(
      "loop-bar",
      [
        ["business", values.business],
        ["api", values.api],
      ],
      direct,
    );
    $("model-conclusion").textContent =
      `在当前手动假设下，消除 3 次模块交接可减去 ${handoff.toFixed(1)} µs。策略计算和原生调用成本仍然保留；这不是实测结果，也不代表真实尾部延迟。`;
  }

  function toast(message) {
    $("toast").textContent = message;
    $("toast").hidden = false;
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => {
      $("toast").hidden = true;
    }, 2600);
  }

  function exportDesign() {
    const assumptions = modelApplied
      ? JSON.stringify(modelValues(), null, 2)
      : "尚未套用任何性能假设；没有实测数据。";
    const content = `# 原生期货执行架构设计\n\n状态：页面为确定性演示；CTP 6.7.11 交易初版已实现并完成本地验证，未连接真实柜台；易达尚未实现。\n\n## 已确认范围\n\n- 中金所 CFFEX：CTP 原生交易。\n- 上期所 SHFE / 上海国际能源交易中心 INE：易达原生交易 SDK。\n- 行情来源独立，暂按 CTP；此次聚焦交易兼容，不设计易达行情。\n- 期货优先；期权不在首期范围。\n\n## 运行结构\n\n每个账户 / 风控域一个实例，显式绑定交易后端与交易所白名单。一个业务 loop 拥有全部交易状态。\n\n事件入口 → 策略意图 → 风控与预占 → 委托 / 持仓状态机 → 原生报单或撤单。\n\n模块间直接调用，不经过 IPC。SDK 内部线程仍依实际接口存在。日志、配置读取、持久化与展示移到后台；持久化可靠性与刷盘策略另行确定。\n\n## 交易兼容约束\n\n- 保留账户、交易所、合约、方向、开平、手数、价格 tick 与有效方式。\n- 上期所 / INE 显式处理平今、平昨与子单分配。\n- 报单前预占；API 本地返回不等于柜台确认或成交。\n- 撤单请求不立即释放冻结量；成交与最终拒单 / 撤单分别核销。\n- 原始订单、成交身份和错误码随事件保留；重复事实幂等处理。\n- 恢复屏障满足前暂停新开仓；入口溢出必须显式使状态进入待恢复。\n- CTP / 易达不跨柜台自动改投。\n\n## 当前页面选择\n\n交易后端：${profile().name}\n交易所：${venue()}\n演示场景：${$("scenario").selectedOptions[0].textContent}\n\n## 手动性能假设（单位 µs；非实测）\n\n${assumptions}\n\n参考分拆路径 = 本地业务处理 + 3 × (IPC + 排队/调度) + 原生 API 调用。\n单 loop = 本地业务处理 + 原生 API 调用。\n网络、SDK 内部排队、柜台穿透与撮合均不包含在此模型中。\n\n## 易达证据与待确认项\n\n官方产品页 https://www.hanlinit.com/products/ydserver/ 列明上期所、能源所支持普通限价 / FAK / FOK，不支持 TAS。\nAPI 文档 https://www.hanlinit.com/docs/ 与下载 https://www.hanlinit.com/downloads/ 要求登录。\n需要取得授权 SDK，核实 ABI、调用方式、线程模型、身份字段、重放 / 查询能力与恢复屏障，再实现适配器。\n官网柜台穿透数字不作为本系统端到端性能或目标。\n`;
    const url = URL.createObjectURL(
      new Blob([content], { type: "text/markdown;charset=utf-8" }),
    );
    const link = el("a");
    link.href = url;
    link.download = "native-futures-execution-design.md";
    document.body.append(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    toast("设计说明已导出，包含当前选择与性能假设。");
  }

  document
    .querySelectorAll("[data-backend]")
    .forEach((button) =>
      button.addEventListener("click", () =>
        setBackend(button.dataset.backend),
      ),
    );
  document
    .querySelectorAll("[data-node]")
    .forEach((button) =>
      button.addEventListener("click", () => selectNode(button.dataset.node)),
    );
  $("venue").addEventListener("change", reset);
  $("scenario").addEventListener("change", () => {
    if ($("scenario").value === "close" && backend !== "yd") {
      setBackend("yd");
      toast("平今 / 平昨拆单场景已切换到易达 · 上期所。");
    } else reset();
  });
  $("play").addEventListener("click", play);
  $("step").addEventListener("click", () => {
    stop();
    advance();
  });
  $("reset").addEventListener("click", reset);
  ["business", "ipc", "schedule", "api"].forEach((key) =>
    $("cost-" + key).addEventListener("input", renderModel),
  );
  $("apply-model").addEventListener("click", () => {
    modelApplied = true;
    renderModel();
  });
  $("export-design").addEventListener("click", exportDesign);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) stop();
  });

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        document.querySelectorAll(".nav-link").forEach((link) => {
          const selected = link.hash === "#" + entry.target.id;
          link.classList.toggle("active", selected);
          if (selected) link.setAttribute("aria-current", "location");
          else link.removeAttribute("aria-current");
        });
      }
    },
    { rootMargin: "-10% 0px -55% 0px", threshold: 0 },
  );
  ["architecture", "compatibility", "events", "latency"].forEach((id) =>
    observer.observe($(id)),
  );
  reset();
  renderModel();
})();
