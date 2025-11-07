   fn handle_trade_response(&mut self, outcome: &TradeExecOutcome) {
        if self.is_strategy_order(outcome.client_order_id) {
            //判断是以下哪个成交回报
            match outcome.req_type {
                TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceNewUMOrder => {
                    debug!(
                        "{}: strategy_id={} trade_response req_type={:?} client_order_id={} status={} body_len={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        outcome.req_type,
                        outcome.client_order_id,
                        outcome.status,
                        outcome.body.len()
                    );

                    let success = (200..300).contains(&outcome.status);
                    let now = get_timestamp_us();
                    let mut final_status: Option<OrderExecutionStatus> = None;

                    let mut manager = self.order_manager.borrow_mut();
                    if !manager.update(outcome.client_order_id, |order| {
                        if success {
                            order.update_status(OrderExecutionStatus::Create);
                            order.set_ack_time(now);
                        } else {
                            order.update_status(OrderExecutionStatus::Rejected);
                            order.set_end_time(now);
                        }
                        final_status = Some(order.status);
                    }) {
                        warn!(
                            "{}: strategy_id={} 未找到 client_order_id={} 对应订单",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.client_order_id
                        );
                    } else if outcome.req_type == TradeRequestType::BinanceNewMarginOrder {
                        if let Some(status) = final_status {
                            if outcome.client_order_id == self.margin_order_id {
                                // 开仓回执
                                if status.is_terminal() && status != OrderExecutionStatus::Filled {
                                    self.margin_order_id = 0;
                                    self.open_timeout_us = None;
                                    self.clear_cancel_wait();
                                    warn!(
                                        "{}: strategy_id={} margin 开仓回执终止 status={} body={}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        status.as_str(),
                                        outcome.body
                                    );
                                }
                                // 平仓回执
                                if status.is_terminal() && status != OrderExecutionStatus::Filled {
                                    warn!(
                                        "{}: strategy_id={} margin 平仓回执终止 status={} body={}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        status.as_str(),
                                        outcome.body
                                    );
                                }
                            } else {
                                debug!(
                                    "{}: strategy_id={} 收到未知 margin 订单回执 client_order_id={} status={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    outcome.client_order_id,
                                    outcome.status
                                );
                            }
                        }
                    }

                    if !success {
                        warn!(
                            "{}: strategy_id={} 下单失败 req_type={:?} status={} body={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.req_type,
                            outcome.status,
                            outcome.body
                        );
                    }
                }
                TradeRequestType::BinanceCancelMarginOrder => {
                    let success = (200..300).contains(&outcome.status);
                    if success {
                        info!(
                            "{}: strategy_id={} margin 撤单成功 client_order_id={} status={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.client_order_id,
                            outcome.status
                        );
                    } else {
                        {
                            let mut manager = self.order_manager.borrow_mut();
                            if !manager.update(outcome.client_order_id, |order| {
                                order.cancel_requested = false;
                            }) {
                                debug!(
                                    "{}: strategy_id={} margin 撤单失败且未找到订单 id={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    outcome.client_order_id
                                );
                            }
                        }

                        warn!(
                            "{}: strategy_id={} margin 撤单失败 status={} body={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.status,
                            outcome.body
                        );
                    }
                }
                _ => {
                    debug!(
                        "{}: strategy_id={} 忽略响应 req_type={:?}",
                        Self::strategy_name(),
                        self.strategy_id,
                        outcome.req_type
                    );
                }
            }
        }
    }



      fn handle_binance_margin_order_update(&mut self, report: &ExecutionReportMsg) {
        let order_id = report.client_order_id;
        if !self.is_strategy_order(order_id) {
            return;
        }

        let status_str = report.order_status.as_str();
        let execution_type = report.execution_type.as_str();

        let mut remove_after_update = false;
        {
            let mut manager = self.order_manager.borrow_mut();
            if !manager.update(order_id, |order| {
                order.set_exchange_order_id(report.order_id);
                match status_str {
                    "NEW" => {
                        order.update_status(OrderExecutionStatus::Create);
                        order.record_exchange_create(report.event_time);
                    } 
                    "PARTIALLY_FILLED" => {
                        order.update_cumulative_filled_quantity(report.cumulative_filled_quantity);
                        order.record_exchange_update(report.event_time);
                    } 
                    "FILLED" => {
                        order.update_status(OrderExecutionStatus::Filled);
                        order.set_filled_time(report.event_time);
                        order.update_cumulative_filled_quantity(report.cumulative_filled_quantity);
                        order.record_exchange_update(report.event_time);
                        // 成功路径不立即移除，等待配对订单也 FILLED 后成对移除
                    }
                    "CANCELED" | "EXPIRED" => {
                        order.update_status(OrderExecutionStatus::Cancelled);
                        order.set_end_time(report.event_time);
                        order.record_exchange_update(report.event_time);
                        remove_after_update = true;
                    } 
                    "REJECTED" | "TRADE_PREVENTION" => {
                        order.update_status(OrderExecutionStatus::Rejected);
                        order.set_end_time(report.event_time);
                        order.record_exchange_update(report.event_time);
                        remove_after_update = true;
                    }
                    _ => {}
                } 
            }) {
                warn!(
                    "{}: strategy_id={} execution_report 未找到订单 id={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_id
                ); 
            } 
        } 

        if remove_after_update {
            self.order_manager.borrow_mut().remove(order_id);
        } 

        if order_id == self.margin_order_id {
            if let Some(order_snapshot) = {
                let manager = self.order_manager.borrow();
                manager.get(order_id)
            } {
                self.log_open_signal_summary(&order_snapshot);
            }
            if status_str == "FILLED" {
                let hedge_delta = {
                    let manager = self.order_manager.borrow();
                    manager
                        .get(order_id)
                        .map(|order| {
                            (order.cumulative_filled_quantity - order.hedged_filled_quantity)
                                .max(0.0)
                        })
                        .unwrap_or(0.0)
                };
                if hedge_delta > 1e-8 {
                    self.order_manager.borrow_mut().update(order_id, |order| {
                        order.hedged_filled_quantity += hedge_delta;
                    });
                    debug!(
                        "{}: strategy_id={} margin order filled, emitting hedge delta={:.6}",
                        Self::strategy_name(),
                        self.strategy_id,
                        hedge_delta
                    );
                    self.emit_hedge_signal(order_id, hedge_delta);
                }
            }

            debug!(
                "{}: strategy_id={} margin execution report status={} execution_type={}",
                Self::strategy_name(),
                self.strategy_id,
                status_str,
                execution_type
            );

            match status_str {
                "FILLED" => {
                    self.open_timeout_us = None;
                    self.clear_cancel_wait();
                    debug!(
                        "{}: strategy_id={} margin 开仓单 FILLED，保留订单供生命周期管理",
                        Self::strategy_name(),
                        self.strategy_id
                    );
                }
                "CANCELED" | "EXPIRED" | "REJECTED" | "TRADE_PREVENTION" => {
                    self.open_timeout_us = None;
                    self.margin_order_id = 0;
                    self.clear_cancel_wait();
                }
                _ => {}
            }
            return;
        }


        debug!(
            "{}: strategy_id={} margin execution report (其他订单) status={} execution_type={}",
            Self::strategy_name(),
            self.strategy_id,
            status_str,
            execution_type
        );
    }


      // handle_binance_futures_order_update
    // 1、处理 UM 对冲单的挂单 / 成交状态
    // 2、处理 UM 平仓单的挂单 / 成交状态
    fn handle_binance_futures_order_update(&mut self, event: &OrderTradeUpdateMsg) {
        let client_order_id = event.client_order_id;
        if !self.is_strategy_order(client_order_id) {
            return;
        }

        let order_label = self.um_order_label(client_order_id);
        debug!(
            "{}: strategy_id={} 收到 {} UM Order Execution Type={} UM Order Status={}",
            Self::strategy_name(),
            self.strategy_id,
            order_label,
            event.execution_type,
            event.order_status
        );

        if event.business_unit != "UM" {
            error!(
                "{}: strategy_id={} {} business_unit 异常={}",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                event.business_unit
            );
        }

        if event.order_type != "MARKET" {
            error!(
                "{}: strategy_id={} {} order_type 异常={}",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                event.order_type
            );
        }

        let mut manager = self.order_manager.borrow_mut();
        let mut outcome = UmOrderUpdateOutcome::Ignored;

        if !manager.update(client_order_id, |order| {
            outcome = Self::apply_um_order_update(order, event);
        }) {
            warn!(
                "{}: strategy_id={} 未找到 {} client_order_id={}，保留激活状态",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                client_order_id
            );
            return;
        }

        drop(manager);

        match outcome {
            UmOrderUpdateOutcome::Created => {
                debug!(
                    "{}: strategy_id={} {} 已确认，等待成交，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::PartiallyFilled(cumulative) => {
                info!(
                    "{}: strategy_id={} {} 部分成交累计数量={:.6}，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    cumulative,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Filled => {
                info!(
                    "{}: strategy_id={} {} 已全部成交，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Expired => {
                warn!(
                    "{}: strategy_id={} {} 收到 EXPIRED 状态，请核实，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Ignored => {
                debug!(
                    "{}: strategy_id={} {} 状态无需处理，UM Order Execution Type={} UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.execution_type,
                    event.order_status
                );
            }
        }

        // 成功路径不立即移除，保留订单直到策略生命周期结束
    }

    fn hanle_period_clock(&mut self, current_tp: i64) {
        // 周期性检查：开仓限价/市价单是否长时间未成交，需要撤单
        if self.mode == StrategyMode::Open {
            if self.margin_order_id == 0 {
                let mut flags = self.period_log_flags.borrow_mut();
                if !flags.margin_open_absent_logged {
                    warn!(
                        "{}: strategy_id={} 当前无 margin 开仓单，策略将等待回收",
                        Self::strategy_name(),
                        self.strategy_id
                    );
                    flags.margin_open_absent_logged = true;
                }
                flags.last_margin_open_status = None;
                self.open_timeout_us = None;
            } else {
                {
                    let mut flags = self.period_log_flags.borrow_mut();
                    flags.margin_open_absent_logged = false;
                }
                let open_order = {
                    let manager = self.order_manager.borrow();
                    manager.get(self.margin_order_id)
                };

                match open_order {
                    Some(order) => {
                        {
                            let mut flags = self.period_log_flags.borrow_mut();
                            flags.margin_open_missing_logged = false;
                        }
                        if order.status.is_terminal() {
                            self.open_timeout_us = None;
                        } else if order.cancel_requested {
                            debug!(
                                "{}: strategy_id={} margin 开仓单撤单已在进行中 order_id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                order.order_id
                            );
                        } else if let (Some(timeout_us), submit_time) =
                            (self.open_timeout_us, order.submit_time)
                        {
                            if submit_time > 0
                                && current_tp.saturating_sub(submit_time) >= timeout_us
                            {
                                info!(
                                    "{}: strategy_id={} margin 开仓单超时，尝试撤单 order_id={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    order.order_id
                                );

                                if let Err(err) =
                                    self.submit_margin_cancel(&order.symbol, order.order_id)
                                {
                                    warn!(
                                        "{}: strategy_id={} margin 开仓撤单失败: {}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        err
                                    );
                                } else {
                                    let mut manager = self.order_manager.borrow_mut();
                                    if !manager.update(order.order_id, |o| {
                                        o.cancel_requested = true;
                                    }) {
                                        warn!(
                                            "{}: strategy_id={} 撤单后更新订单状态失败 id={}",
                                            Self::strategy_name(),
                                            self.strategy_id,
                                            order.order_id
                                        );
                                    }
                                    self.open_timeout_us = None;
                                }
                            }
                        }
                    }
                    None => {
                        {
                            let mut flags = self.period_log_flags.borrow_mut();
                            if !flags.margin_open_missing_logged {
                                warn!(
                                    "{}: strategy_id={} 未找到 margin 开仓单 id={}，清除本地状态",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    self.margin_order_id
                                );
                                flags.margin_open_missing_logged = true;
                            }
                            flags.last_margin_open_status = None;
                            flags.margin_open_absent_logged = false;
                        }
                        self.margin_order_id = 0;
                        self.open_timeout_us = None;
                        self.clear_cancel_wait();
                    }
                }
            }
        } else {
            let mut flags = self.period_log_flags.borrow_mut();
            flags.margin_open_absent_logged = false;
            flags.margin_open_missing_logged = false;
            flags.last_margin_open_status = None;
            self.open_timeout_us = None;
        }
    }


    
fn format_decimal(value: f64) -> String {
    let mut s = format!("{:.8}", value);
    if let Some(dot_pos) = s.find('.') {
        while s.len() > dot_pos + 1 && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn format_quantity(quantity: f64) -> String {
    format_decimal(quantity)
}

fn format_price(price: f64) -> String {
    format_decimal(price)
}

fn align_price_floor(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return value;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((value * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((value / tick) + 1e-9).floor();
    scaled * tick
}

fn align_price_ceil(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return value;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((value * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((value / tick) - 1e-9).ceil();
    scaled * tick
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}
