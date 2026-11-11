#!/usr/bin/env python3
"""Exercise the actual Rust/C++ bridge with a synthetic SDK, never a network front.

The official header supplies the ABI. Unused pure-virtual methods are generated
as failing stubs, so the test does not maintain a parallel hand-written SDK ABI.
This fixture is for tests only, not a CTP-compatible runtime backend.
"""
from native_persistence_check import verify_persistence
import argparse
import json
import os
from pathlib import Path
import queue
import re
import subprocess
import tempfile
import threading
import time
import tomllib


MOCK = r'''
#include "ThostFtdcTraderApi.h"
#include <cstring>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>
#include <stdexcept>
static void check(bool v) { if (!v) throw std::runtime_error("fixture request validation failed"); }
class Fixture final : public CThostFtdcTraderApi {
    CThostFtdcTraderSpi* spi = nullptr;
    std::map<std::string, CThostFtdcOrderField> orders;
    std::vector<CThostFtdcTradeField> trades;
    int long_position = 0, short_position = 0;
    void fill(CThostFtdcOrderField& o, int quantity) {
        CThostFtdcTradeField t{};
        std::strcpy(t.InvestorID, "test"); std::strcpy(t.ExchangeID, "CFFEX");
        std::strcpy(t.InstrumentID, "IF2612"); std::strcpy(t.TradingDay, "20261109");
        std::strcpy(t.OrderRef, o.OrderRef); std::strcpy(t.OrderSysID, o.OrderSysID);
        std::string id = "FILL-" + std::to_string(trades.size() + 1); std::strcpy(t.TradeID, id.c_str());
        t.Direction = o.Direction; t.OffsetFlag = o.CombOffsetFlag[0]; t.HedgeFlag = THOST_FTDC_HF_Speculation;
        t.Volume = quantity; t.Price = o.LimitPrice;
        int& position = t.OffsetFlag == THOST_FTDC_OF_Open
            ? (t.Direction == THOST_FTDC_D_Buy ? long_position : short_position)
            : (t.Direction == THOST_FTDC_D_Buy ? short_position : long_position);
        position += t.OffsetFlag == THOST_FTDC_OF_Open ? quantity : -quantity;
        trades.push_back(t); spi->OnRtnTrade(&t); spi->OnRtnTrade(&t); // Intentional duplicate.
        o.VolumeTraded += quantity; o.VolumeTotal -= quantity;
    }
public:
    void Release() override { delete this; }
    void RegisterSpi(CThostFtdcTraderSpi* p) override { spi = p; }
    void RegisterFront(char* p) override { check(std::strcmp(p, "tcp://127.0.0.1:1") == 0); }
    void SubscribePrivateTopic(THOST_TE_RESUME_TYPE mode) override { check(mode == THOST_TERT_RESUME); }
    void SubscribePublicTopic(THOST_TE_RESUME_TYPE mode) override { check(mode == THOST_TERT_RESTART); }
    void Init() override {
        spi->OnFrontConnected(); CThostFtdcInstrumentStatusField s{};
        std::strcpy(s.ExchangeID, "CFFEX"); std::strcpy(s.InstrumentID, "IF");
        s.InstrumentStatus = THOST_FTDC_IS_Continous; spi->OnRtnInstrumentStatus(&s);
    }
    int ReqAuthenticate(CThostFtdcReqAuthenticateField* p, int id) override {
        check(std::strcmp(p->BrokerID, "TEST") == 0 && std::strcmp(p->UserID, "test") == 0);
        check(std::strcmp(p->AppID, "fixture") == 0 && std::strcmp(p->AuthCode, "fixture") == 0);
        spi->OnRspAuthenticate(nullptr, nullptr, id, true); return 0;
    }
    int ReqUserLogin(CThostFtdcReqUserLoginField* p, int id) override {
        check(std::strcmp(p->Password, "fixture-only") == 0);
        CThostFtdcRspUserLoginField r{}; r.FrontID = 7; r.SessionID = 11;
        std::strcpy(r.TradingDay, "20261109"); std::strcpy(r.MaxOrderRef, "100");
        spi->OnRspUserLogin(&r, nullptr, id, true); return 0;
    }
    int ReqSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField* p, int id) override {
        check(std::strcmp(p->InvestorID, "test") == 0); spi->OnRspSettlementInfoConfirm(p, nullptr, id, true); return 0;
    }
    int ReqQryInstrument(CThostFtdcQryInstrumentField* p, int id) override {
        check(std::strlen(p->ExchangeID) == 0); CThostFtdcInstrumentField r{};
        std::strcpy(r.InstrumentID, "IF2612"); std::strcpy(r.ExchangeID, "CFFEX");
        r.VolumeMultiple=300; std::strcpy(r.ExpireDate,"20261218"); std::strcpy(r.ProductID,"IF"); r.PriceTick = .2; r.IsTrading = 1; r.ProductClass = THOST_FTDC_PC_Futures;
        r.MinLimitOrderVolume = 1; r.MaxLimitOrderVolume = 100;
        spi->OnRspQryInstrument(&r, nullptr, id, true); return 0;
    }
    int ReqQrySettlementInfoConfirm(CThostFtdcQrySettlementInfoConfirmField*, int id) override {
        CThostFtdcSettlementInfoConfirmField r{};std::strcpy(r.InvestorID,"test");std::strcpy(r.ConfirmDate,"20261109");
        if(std::getenv("CTP_FIXTURE_UNCONFIRMED"))spi->OnRspQrySettlementInfoConfirm(nullptr,nullptr,id,true);
        else spi->OnRspQrySettlementInfoConfirm(&r,nullptr,id,true);return 0;
    }
    int ReqQryInstrumentCommissionRate(CThostFtdcQryInstrumentCommissionRateField* p,int id) override {
        check(std::strcmp(p->ExchangeID,"CFFEX")==0 && std::strcmp(p->InstrumentID,"IF2612")==0);
        CThostFtdcInstrumentCommissionRateField r{};std::strcpy(r.ExchangeID,"CFFEX");std::strcpy(r.InstrumentID,"IF2612");std::strcpy(r.InvestorID,"test");r.InvestorRange=THOST_FTDC_IR_Single;
        r.OpenRatioByMoney=.000023;r.CloseRatioByMoney=.000023;r.CloseTodayRatioByMoney=.00023;
        spi->OnRspQryInstrumentCommissionRate(&r,nullptr,id,true);return 0;
    }
    int ReqQryInstrumentMarginRate(CThostFtdcQryInstrumentMarginRateField* p,int id) override {
        check(std::strcmp(p->ExchangeID,"CFFEX")==0 && std::strcmp(p->InstrumentID,"IF2612")==0 && p->HedgeFlag==THOST_FTDC_HF_Speculation);
        CThostFtdcInstrumentMarginRateField r{};std::strcpy(r.ExchangeID,"CFFEX");std::strcpy(r.InstrumentID,"IF2612");std::strcpy(r.InvestorID,"test");r.InvestorRange=THOST_FTDC_IR_Single;r.HedgeFlag=THOST_FTDC_HF_Speculation;r.IsRelative=1;
        r.LongMarginRatioByMoney=.02;r.ShortMarginRatioByMoney=.03;spi->OnRspQryInstrumentMarginRate(&r,nullptr,id,true);return 0;
    }
    int ReqQryExchangeMarginRate(CThostFtdcQryExchangeMarginRateField* p,int id) override {
        check(std::strcmp(p->ExchangeID,"CFFEX")==0 && std::strcmp(p->InstrumentID,"IF2612")==0);
        CThostFtdcExchangeMarginRateField r{};std::strcpy(r.ExchangeID,"CFFEX");std::strcpy(r.InstrumentID,"IF2612");r.HedgeFlag=THOST_FTDC_HF_Speculation;r.LongMarginRatioByMoney=.10;r.ShortMarginRatioByMoney=.10;
        spi->OnRspQryExchangeMarginRate(&r,nullptr,id,true);return 0;
    }
    int ReqQryTradingAccount(CThostFtdcQryTradingAccountField* p, int id) override {
        check(std::strcmp(p->CurrencyID, "CNY") == 0); CThostFtdcTradingAccountField r{};
        std::strcpy(r.AccountID, "test"); std::strcpy(r.CurrencyID, "CNY"); std::strcpy(r.TradingDay, "20261109");
        r.Available = 1000000; r.Balance=1200000; r.CurrMargin=200000; r.Commission=52.5; spi->OnRspQryTradingAccount(&r, nullptr, id, true); return 0;
    }
    int ReqQryInvestorPosition(CThostFtdcQryInvestorPositionField*, int id) override {
        for (int direction : {THOST_FTDC_PD_Long, THOST_FTDC_PD_Short}) {
            const int quantity = direction == THOST_FTDC_PD_Long ? long_position : short_position;
            if (!quantity) continue;
            CThostFtdcInvestorPositionField r{};
            std::strcpy(r.InvestorID, "test"); std::strcpy(r.InstrumentID, "IF2612"); std::strcpy(r.ExchangeID, "CFFEX");
            r.HedgeFlag = THOST_FTDC_HF_Speculation; r.PosiDirection = direction;
            r.PositionDate = THOST_FTDC_PSD_Today; r.Position = quantity; r.TodayPosition = quantity;
            spi->OnRspQryInvestorPosition(&r, nullptr, id, false);
        }
        spi->OnRspQryInvestorPosition(nullptr, nullptr, id, true); return 0;
    }
    int ReqQryOrder(CThostFtdcQryOrderField*, int id) override {
        for (auto& pair : orders) spi->OnRspQryOrder(&pair.second, nullptr, id, false);
        spi->OnRspQryOrder(nullptr, nullptr, id, true); return 0;
    }
    int ReqQryTrade(CThostFtdcQryTradeField*, int id) override {
        for (auto& t : trades) spi->OnRspQryTrade(&t, nullptr, id, false);
        spi->OnRspQryTrade(nullptr, nullptr, id, true); return 0;
    }
    int ReqOrderInsert(CThostFtdcInputOrderField* p, int id) override {
        check(p->RequestID == id && std::strcmp(p->InvestorID, "test") == 0 && std::strcmp(p->UserID, "test") == 0);
        check(std::strcmp(p->ExchangeID, "CFFEX") == 0 && std::strcmp(p->InstrumentID, "IF2612") == 0);
        check(p->OrderPriceType == THOST_FTDC_OPT_LimitPrice && p->CombHedgeFlag[0] == THOST_FTDC_HF_Speculation);
        check(p->CombOffsetFlag[0] == THOST_FTDC_OF_Open || p->CombOffsetFlag[0] == THOST_FTDC_OF_Close || p->CombOffsetFlag[0] == THOST_FTDC_OF_CloseToday || p->CombOffsetFlag[0] == THOST_FTDC_OF_CloseYesterday);
        if (p->LimitPrice > 5000) { CThostFtdcRspInfoField error{}; error.ErrorID = 31; spi->OnRspOrderInsert(p, &error, id, true); return 0; }
        CThostFtdcOrderField o{};
        std::strcpy(o.InvestorID, p->InvestorID); std::strcpy(o.ExchangeID, p->ExchangeID);
        std::strcpy(o.InstrumentID, p->InstrumentID); std::strcpy(o.OrderRef, p->OrderRef);
        const std::string sys = "SYS-" + std::string(p->OrderRef); std::strcpy(o.OrderSysID, sys.c_str());
        std::strcpy(o.TradingDay, "20261109"); o.FrontID = 7; o.SessionID = 11;
        o.Direction = p->Direction; o.CombOffsetFlag[0] = p->CombOffsetFlag[0]; o.CombHedgeFlag[0] = p->CombHedgeFlag[0];
        o.VolumeTotalOriginal = p->VolumeTotalOriginal; o.VolumeTotal = p->VolumeTotalOriginal; o.LimitPrice = p->LimitPrice;
        o.TimeCondition = p->TimeCondition; o.VolumeCondition = p->VolumeCondition;
        o.OrderStatus = THOST_FTDC_OST_NoTradeQueueing; o.OrderSubmitStatus = THOST_FTDC_OSS_Accepted;
        orders[o.OrderRef] = o; auto& stored = orders.at(o.OrderRef);
        if (p->TimeCondition == THOST_FTDC_TC_IOC) {
            check(p->VolumeCondition == THOST_FTDC_VC_AV || (p->VolumeCondition == THOST_FTDC_VC_CV && p->MinVolume == p->VolumeTotalOriginal));
            fill(stored, p->VolumeTotalOriginal); stored.OrderStatus = THOST_FTDC_OST_AllTraded;
        } else check(p->TimeCondition == THOST_FTDC_TC_GFD && p->VolumeCondition == THOST_FTDC_VC_AV && p->MinVolume == 1);
        spi->OnRtnOrder(&stored); return 0;
    }
    int ReqOrderAction(CThostFtdcInputOrderActionField* p, int id) override {
        check(p->RequestID == id && p->OrderActionRef == id && p->ActionFlag == THOST_FTDC_AF_Delete);
        check(p->FrontID == 7 && p->SessionID == 11 && std::strcmp(p->UserID, "test") == 0);
        auto& order = orders.at(p->OrderRef); check(std::strcmp(p->OrderSysID, order.OrderSysID) == 0);
        fill(order, 1); order.OrderStatus = THOST_FTDC_OST_Canceled; spi->OnRtnOrder(&order); return 0;
    }
    // GENERATED_STUBS
};
CThostFtdcTraderApi* CThostFtdcTraderApi::CreateFtdcTraderApi(const char*, bool production) { check(!production); return new Fixture; }
const char* CThostFtdcTraderApi::GetApiVersion() { return "v6.7.11_fixture"; }
'''


def fixture_source(sdk):
    header = (sdk / "ThostFtdcTraderApi.h").read_text(encoding="utf-8", errors="replace")
    implemented = set(re.findall(r"\b(\w+)\([^;{}]*\)\s*override\s*\{", MOCK))
    stubs = []
    for signature in re.findall(r"virtual\s+([^;\n]+?)\s*=\s*0\s*;", header):
        name = re.search(r"(\w+)\s*\(", signature).group(1)
        if name not in implemented:
            result = "" if signature.startswith("void ") else "return -9901;" if signature.startswith("int ") else "return {};"
            stubs.append(signature + " override { " + result + " }")
    return MOCK.replace("// GENERATED_STUBS", "\n".join(stubs))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--venue", choices=["CFFEX","SHFE","INE","DCE","CZCE","GFEX"], default="CFFEX")
    parser.add_argument("--symbol", default="IF2612")
    parser.add_argument("--unconfirmed-settlement",action="store_true")
    parser.add_argument("--transport-timeout",action="store_true")
    parser.add_argument("--placement-config",type=Path,required=True,help="explicit CPU/NUMA placement for the authorized external host")
    args = parser.parse_args()
    placement=tomllib.loads(args.placement_config.read_text())
    required={"numa_node","business_cpu","sdk_cpus","background_cpus"}
    if set(placement)!=required: raise ValueError("placement config must contain exactly NUMA node and three CPU domains")
    placement_table="[placement]\n"+"\n".join(k+" = "+json.dumps(placement[k]) for k in ["numa_node","business_cpu","sdk_cpus","background_cpus"])+"\n"
    root = Path(__file__).resolve().parents[3]
    with tempfile.TemporaryDirectory(prefix="mkt-ctp-bridge-fixture-") as directory:
        temp = Path(directory)
        source, library = temp / "fixture.cpp", temp / "fixture.so"
        product = args.symbol.rstrip("0123456789")
        fixture = fixture_source(args.sdk_dir).replace('"CFFEX"', json.dumps(args.venue)).replace('"IF2612"', json.dumps(args.symbol)).replace('"IF"',json.dumps(product))
        if args.transport_timeout: fixture=re.sub(r"void Init\(\) override \{.*?\n    \}","void Init() override {}",fixture,flags=re.S)
        source.write_text(fixture)
        subprocess.run(["c++", "-std=c++17", "-shared", "-fPIC", "-isystem", str(args.sdk_dir), str(source), "-o", str(library)], check=True)
        config = (root / "config/ctp_trade.example.toml").read_text()
        config=config[:config.index("[placement]")]+placement_table
        for old, new in [('/opt/ctp/6.7.11/libthosttraderapi_se.so', str(library)), ('/tmp/mkt_ctp_sim/flow', str(temp / 'flow')), ('/tmp/mkt_ctp_sim/events.jsonl', str(temp / 'events.jsonl')), ('broker_id = "REPLACE"', 'broker_id = "TEST"'), ('cash_budget_per_open_lot = 250000.0', 'cash_budget_per_open_lot = 10000.0'), ('max_order_lots = 1', 'max_order_lots = 10'), ('max_working_lots = 2', 'max_working_lots = 10'), ('max_actions_per_second = 2', 'max_actions_per_second = 20'), ('enable_fak_fok = false', 'enable_fak_fok = true')]:
            config = config.replace(old, new)
        config=config.replace('venue = "CFFEX"',f'venue = "{args.venue}"').replace('symbol = "IF2612"',f'symbol = "{args.symbol}"')
        if args.transport_timeout: config=config.replace("response_timeout_ms = 30000","response_timeout_ms = 5000").replace("reconnect_backoff_ms = 5000","reconnect_backoff_ms = 1000")
        config_path = temp / "config.toml"
        config_path.write_text(config)
        environment = dict(os.environ, CTP_USER_ID="test", CTP_PASSWORD="fixture-only", CTP_APP_ID="fixture", CTP_AUTH_CODE="fixture")
        if args.unconfirmed_settlement: environment["CTP_FIXTURE_UNCONFIRMED"]="1"
        stderr = open(temp / "stderr.log", "w+")
        process = subprocess.Popen([str(args.binary.resolve()), "--config", str(config_path), "--connect", "--execute"], env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, text=True)
        responses = queue.Queue()
        threading.Thread(target=lambda: [responses.put(json.loads(line)) for line in process.stdout], daemon=True).start()

        def send(value):
            process.stdin.write(json.dumps(value) + "\n")
            process.stdin.flush()

        def wait(kind, timeout=30):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    value = responses.get(timeout=min(1, max(.01, deadline - time.monotonic())))
                except queue.Empty:
                    if process.poll() is not None:
                        stderr.seek(0)
                        raise AssertionError(stderr.read())
                    continue
                if value.get("type") == kind:
                    return value
                if value.get("type") in ("trading_blocked", "command_rejected"):
                    raise AssertionError(value)
            raise AssertionError(f"timed out waiting for {kind}")

        def state():
            send({"op": "state"})
            result=wait("state")
            assert result["account"]["balance"]==1200000 and result["account"]["commission"]==52.5
            return result["state"]

        def submit(tif="day", side="buy", offset="open", lots=1, ticks=20000):
            send({"op": "submit", "intent": {"venue": args.venue, "symbol": args.symbol, "side": side, "offset": offset, "lots": lots, "price_ticks": ticks, "time_in_force": tif}})
            receipt = wait("submit_receipt")["receipt"]
            assert receipt["all_submitted_locally"]
            return receipt["children"][0]["key"]

        try:
            if args.transport_timeout:
                wait("reconnecting",timeout=15)
                send({"op":"state"});snapshot=wait("state")
                assert snapshot["account"] is None and not snapshot["state"]["ready"]
                send({"op":"shutdown"});wait("runtime_stopped");assert process.wait(timeout=10)==0
                events=[json.loads(line) for line in (temp/"events.jsonl").read_text().splitlines()]
                assert not any(e.get("type")=="prepared_submit" or e.get("event",{}).get("kind")==5 for e in events)
                print(json.dumps({"transport_timeout":True,"reconnect_backoff":True,"trade_blocked":True,"settlement_confirmations":0,"orders_sent":0}))
                return
            if args.unconfirmed_settlement:
                assert not wait("reconciled")["state"]["ready"]
                assert state()["instruments"][0]["contract_multiple"]==300
                send({"op":"submit","intent":{"venue":args.venue,"symbol":args.symbol,"side":"buy","offset":"open","lots":1,"price_ticks":20000,"time_in_force":"day"}})
                wait("command_rejected")
                send({"op":"shutdown"});wait("runtime_stopped");assert process.wait(timeout=10)==0
                events=[json.loads(line) for line in (temp/"events.jsonl").read_text().splitlines()]
                assert not any(e.get("type")=="prepared_submit" or e.get("event",{}).get("kind")==5 for e in events)
                print(json.dumps({"unconfirmed_settlement":True,"account_and_rules_readable":True,"trade_blocked":True,"settlement_confirmations":0,"orders_sent":0}))
                return
            assert wait("reconciled")["state"]["ready"]
            rule=state()["instruments"][0]
            assert rule["contract_multiple"]==300 and rule["expiry_date"]==20261218
            assert abs(rule["margin"]["long_by_money"]-.12)<1e-12
            assert rule["commission"]["close_today_by_money"]==.00023
            day = submit(lots=2)
            send({"op": "cancel", "key": day})
            wait("cancel_submitted_locally")
            assert next(o for o in state()["orders"] if o["key"] == day)["status"] == "cancelled"
            submit("fak")
            submit("fok", side="sell")
            positions = state()["positions"]
            assert next(p for p in positions if p["side"] == "buy")["position"]["today"] == 2
            assert next(p for p in positions if p["side"] == "sell")["position"]["today"] == 1
            close = submit(side="sell", offset="close", lots=2)
            send({"op": "cancel", "key": close})
            wait("cancel_submitted_locally")
            positions = state()["positions"]
            assert next(p for p in positions if p["side"] == "buy")["position"]["today"] == 1
            rejected = submit(ticks=30000)
            assert next(o for o in state()["orders"] if o["key"] == rejected)["status"] == "rejected"
            send({"op": "reconcile"})
            wait("reconciling")
            restored = wait("reconciled")["state"]
            assert restored["ready"]
            assert next(p for p in restored["positions"] if p["side"] == "buy")["position"]["today"] == 1
            after_recovery = submit("fak")
            assert int(after_recovery["order_ref"]) > int(rejected["order_ref"])
            send({"op": "shutdown"})
            wait("runtime_stopped")
            assert process.wait(timeout=10) == 0
            events = [json.loads(line) for line in (temp / "events.jsonl").read_text().splitlines()]
            assert any(e.get("event", {}).get("error_code") == 31 for e in events)
            assert not any(e.get("event",{}).get("kind")==5 for e in events)
            assert not any(e.get("type") == "trading_blocked" for e in events)
            assert "fixture-only" not in (temp / "events.jsonl").read_text()
            print(json.dumps({"bridge": "official header ABI + synthetic SDK", "native_network_connections": 0, "venue":args.venue, "bootstrap": True, "limit_cancel": True, "fak_fok": True, "duplicate_fills": True, "close": True, "async_rejection": True, "reconcile": True, "journal_events": len(events)}))
        finally:
            if process.returncode == 0:
                persisted = verify_persistence(temp / "flow" / "persist_manager", args.binary.resolve().parent / "cn_order_export", temp / "db-export")
                print(json.dumps(persisted))
            if process.poll() is None:
                process.kill()
                process.wait()
            stderr.close()


if __name__ == "__main__":
    main()
