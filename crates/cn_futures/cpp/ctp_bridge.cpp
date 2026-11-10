#include "ctp_bridge.h"
#include "ThostFtdcTraderApi.h"
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace {
static_assert(std::is_standard_layout_v<NfEvent>);
constexpr int invalid = -9901;
template <size_t N> void copy(char (&out)[N], const char* text) {
    if (!text || std::strlen(text) >= N) throw std::invalid_argument("CTP field length exceeds SDK capacity");
    std::memcpy(out, text, std::strlen(text) + 1);
}
template <size_t N, size_t M> void field(char (&out)[N], const char (&in)[M]) {
    const size_t length = strnlen(in, M);
    const size_t n = length < N - 1 ? length : N - 1;
    std::memcpy(out, in, n); out[n] = 0;
}
void message(char* out, size_t cap, const char* text) {
    if (out && cap) std::snprintf(out, cap, "%s", text ? text : "CTP bridge failure");
}
struct Library {
    void* handle;
    explicit Library(const char* path) : handle(dlopen(path, RTLD_NOW | RTLD_LOCAL)) {
        if (!handle) throw std::runtime_error(dlerror());
    }
    ~Library() { if (handle) dlclose(handle); }
    template <typename T> T symbol(const char* name) {
        dlerror(); auto p = dlsym(handle, name);
        if (const char* e = dlerror()) throw std::runtime_error(e);
        return reinterpret_cast<T>(p);
    }
};

class Spi final : public CThostFtdcTraderSpi {
    NfCallback callback; void* context;
    void send(NfEvent& e, CThostFtdcRspInfoField* info = nullptr, int request = 0, bool last = true) {
        e.request = request; e.last = last;
        if (info) { e.error = info->ErrorID; field(e.message, info->ErrorMsg); }
        callback(context, &e);
    }
    static void order(NfEvent& e, CThostFtdcOrderField* p) {
        if (!p) return;
        e.present = 1;
        field(e.symbol, p->InstrumentID); field(e.exchange, p->ExchangeID);
        field(e.order_ref, p->OrderRef); field(e.system_id, p->OrderSysID);
        field(e.trading_day, p->TradingDay); field(e.investor, p->InvestorID);
        field(e.message, p->StatusMsg); e.front = p->FrontID; e.session = p->SessionID;
        e.volume = p->VolumeTotalOriginal; e.traded = p->VolumeTraded; e.remaining = p->VolumeTotal;
        e.time_condition = p->TimeCondition; e.volume_condition = p->VolumeCondition;
        e.price = p->LimitPrice; e.status = p->OrderStatus; e.submit_status = p->OrderSubmitStatus;
        e.direction = p->Direction; e.offset = p->CombOffsetFlag[0]; e.hedge = p->CombHedgeFlag[0];
    }
    static void trade(NfEvent& e, CThostFtdcTradeField* p) {
        if (!p) return;
        e.present = 1;
        field(e.symbol, p->InstrumentID); field(e.exchange, p->ExchangeID);
        field(e.order_ref, p->OrderRef); field(e.system_id, p->OrderSysID); field(e.trade_id, p->TradeID);
        field(e.trading_day, p->TradingDay); field(e.investor, p->InvestorID);
        e.direction = p->Direction; e.offset = p->OffsetFlag; e.hedge = p->HedgeFlag;
        e.volume = p->Volume; e.price = p->Price;
    }
public:
    Spi(NfCallback cb, void* ctx) : callback(cb), context(ctx) {}
    void OnFrontConnected() override { NfEvent e{}; e.kind = 1; send(e); }
    void OnFrontDisconnected(int reason) override { NfEvent e{}; e.kind = 2; e.status = reason; send(e); }
    void OnRspAuthenticate(CThostFtdcRspAuthenticateField*, CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.kind = 3; send(e, i, r, l); }
    void OnRspUserLogin(CThostFtdcRspUserLoginField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.kind = 4;
        if (p) { e.present = 1; e.front = p->FrontID; e.session = p->SessionID; field(e.max_order_ref, p->MaxOrderRef); field(e.trading_day, p->TradingDay); }
        send(e, i, r, l);
    }
    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField*, CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.kind = 5; send(e, i, r, l); }
    void OnRspQryInstrument(CThostFtdcInstrumentField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.kind = 6;
        if (p) { e.present = 1; field(e.symbol, p->InstrumentID); field(e.exchange, p->ExchangeID); e.price_tick = p->PriceTick; e.active = p->IsTrading; e.product_class = p->ProductClass; e.minimum = p->MinLimitOrderVolume; e.maximum = p->MaxLimitOrderVolume; }
        send(e, i, r, l);
    }
    void OnRspQryTradingAccount(CThostFtdcTradingAccountField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.kind = 7;
        if (p) { e.present = 1; e.available = p->Available; field(e.investor, p->AccountID); field(e.trading_day, p->TradingDay); field(e.exchange, p->CurrencyID); }
        send(e, i, r, l);
    }
    void OnRspQryInvestorPosition(CThostFtdcInvestorPositionField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.kind = 8;
        if (p) { e.present = 1; field(e.symbol, p->InstrumentID); field(e.exchange, p->ExchangeID); field(e.investor, p->InvestorID); field(e.trading_day, p->TradingDay); e.volume = p->Position; e.today = p->TodayPosition; e.yesterday = p->YdPosition; e.hedge = p->HedgeFlag; e.position_direction = p->PosiDirection; e.position_date = p->PositionDate; }
        send(e, i, r, l);
    }
    void OnRtnOrder(CThostFtdcOrderField* p) override { NfEvent e{}; e.kind = 9; order(e, p); send(e); }
    void OnRtnTrade(CThostFtdcTradeField* p) override { NfEvent e{}; e.kind = 10; trade(e, p); send(e); }
    void OnRspOrderInsert(CThostFtdcInputOrderField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.kind = 11;
        if (p) { e.present = 1; field(e.order_ref, p->OrderRef); field(e.symbol, p->InstrumentID); }
        send(e, i, r, l);
    }
    void OnErrRtnOrderInsert(CThostFtdcInputOrderField* p, CThostFtdcRspInfoField* i) override { OnRspOrderInsert(p, i, p ? p->RequestID : 0, true); }
    void OnRspOrderAction(CThostFtdcInputOrderActionField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.kind = 12;
        if (p) { e.present = 1; e.front = p->FrontID; e.session = p->SessionID; field(e.order_ref, p->OrderRef); field(e.system_id, p->OrderSysID); }
        send(e, i, r, l);
    }
    void OnErrRtnOrderAction(CThostFtdcOrderActionField* p, CThostFtdcRspInfoField* i) override {
        NfEvent e{}; e.kind = 12;
        if (p) { e.present = 1; e.front = p->FrontID; e.session = p->SessionID; field(e.order_ref, p->OrderRef); field(e.system_id, p->OrderSysID); }
        send(e, i, p ? p->RequestID : 0);
    }
    void OnRspQryOrder(CThostFtdcOrderField* p, CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.kind = 13; order(e, p); send(e, i, r, l); }
    void OnRspQryTrade(CThostFtdcTradeField* p, CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.kind = 14; trade(e, p); send(e, i, r, l); }
    void OnRtnInstrumentStatus(CThostFtdcInstrumentStatusField* p) override {
        NfEvent e{}; e.kind = 15;
        if (p) { e.present = 1; field(e.symbol, p->InstrumentID); field(e.exchange, p->ExchangeID); e.status = p->InstrumentStatus; }
        send(e);
    }
    void OnRspError(CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.kind = 16; send(e, i, r, l); }
};
struct Handle {
    Library library; Spi spi; CThostFtdcTraderApi* api = nullptr; std::string front, user;
    Handle(const char* path, const char* flow, bool production, NfCallback cb, void* ctx) : library(path), spi(cb, ctx) {
        auto version = library.symbol<decltype(&CThostFtdcTraderApi::GetApiVersion)>("_ZN19CThostFtdcTraderApi13GetApiVersionEv");
        const char* v = version();
        const char* numeric = v && v[0] == 'v' ? v + 1 : v;
        if (!numeric || std::strncmp(numeric, "6.7.11", 6) || (numeric[6] && numeric[6] != '_' && numeric[6] != '.')) throw std::runtime_error("SDK library must match the compiled official CTP 6.7.11 headers");
        auto create = library.symbol<decltype(&CThostFtdcTraderApi::CreateFtdcTraderApi)>("_ZN19CThostFtdcTraderApi19CreateFtdcTraderApiEPKcb");
        api = create(flow, production); if (!api) throw std::runtime_error("CTP factory returned null");
        api->RegisterSpi(&spi);
    }
    ~Handle() { if (api) { api->RegisterSpi(nullptr); api->Release(); } }
};
template <typename F> int request(void* h, F f) {
    try { if (!h) return invalid; return f(*static_cast<Handle*>(h)); } catch (...) { return invalid; }
}
}

extern "C" size_t nf_ctp_event_size() { return sizeof(NfEvent); }
extern "C" int nf_ctp_version(const char* path, char* out, size_t cap) {
    try { Library lib(path); auto f = lib.symbol<decltype(&CThostFtdcTraderApi::GetApiVersion)>("_ZN19CThostFtdcTraderApi13GetApiVersionEv"); message(out, cap, f()); return 0; }
    catch (const std::exception& e) { message(out, cap, e.what()); return invalid; }
}
extern "C" void* nf_ctp_create(const char* path, const char* flow, bool production, NfCallback cb, void* ctx, char* error, size_t cap) {
    try { return new Handle(path, flow, production, cb, ctx); }
    catch (const std::exception& e) { message(error, cap, e.what()); return nullptr; }
    catch (...) { message(error, cap, "CTP construction failed"); return nullptr; }
}
extern "C" void nf_ctp_destroy(void* h) { delete static_cast<Handle*>(h); }
extern "C" int nf_ctp_start(void* h, const char* front) {
    return request(h, [&](Handle& x) { x.front = front; x.api->SubscribePrivateTopic(THOST_TERT_RESUME); x.api->SubscribePublicTopic(THOST_TERT_RESTART); x.api->RegisterFront(x.front.data()); x.api->Init(); return 0; });
}
extern "C" int nf_ctp_authenticate(void* h, int id, const char* broker, const char* user, const char* app, const char* auth) {
    return request(h, [&](Handle& x) { CThostFtdcReqAuthenticateField r{}; copy(r.BrokerID, broker); copy(r.UserID, user); copy(r.AppID, app); copy(r.AuthCode, auth); return x.api->ReqAuthenticate(&r, id); });
}
extern "C" int nf_ctp_login(void* h, int id, const char* broker, const char* user, const char* password) {
    return request(h, [&](Handle& x) { CThostFtdcReqUserLoginField r{}; copy(r.BrokerID, broker); copy(r.UserID, user); copy(r.Password, password); x.user = user; return x.api->ReqUserLogin(&r, id); });
}
extern "C" int nf_ctp_settlement(void* h, int id, const char* broker, const char* investor) {
    return request(h, [&](Handle& x) { CThostFtdcSettlementInfoConfirmField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); return x.api->ReqSettlementInfoConfirm(&r, id); });
}
extern "C" int nf_ctp_query(void* h, int kind, int id, const char* broker, const char* investor) {
    return request(h, [&](Handle& x) {
        switch (kind) {
        case 6: { CThostFtdcQryInstrumentField r{}; copy(r.ExchangeID, "CFFEX"); return x.api->ReqQryInstrument(&r, id); }
        case 7: { CThostFtdcQryTradingAccountField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.CurrencyID, "CNY"); return x.api->ReqQryTradingAccount(&r, id); }
        case 8: { CThostFtdcQryInvestorPositionField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.ExchangeID, "CFFEX"); return x.api->ReqQryInvestorPosition(&r, id); }
        case 13: { CThostFtdcQryOrderField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.ExchangeID, "CFFEX"); return x.api->ReqQryOrder(&r, id); }
        case 14: { CThostFtdcQryTradeField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.ExchangeID, "CFFEX"); return x.api->ReqQryTrade(&r, id); }
        default: return invalid;
        }
    });
}
extern "C" int nf_ctp_insert(void* h, int id, const char* broker, const char* investor, const char* symbol, const char* ref, int direction, int offset, int tif, int lots, double price) {
    return request(h, [&](Handle& x) {
        CThostFtdcInputOrderField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.UserID, x.user.c_str()); copy(r.InstrumentID, symbol); copy(r.OrderRef, ref); copy(r.ExchangeID, "CFFEX");
        r.Direction = static_cast<char>(direction); r.CombOffsetFlag[0] = static_cast<char>(offset); r.CombHedgeFlag[0] = THOST_FTDC_HF_Speculation;
        r.OrderPriceType = THOST_FTDC_OPT_LimitPrice; r.LimitPrice = price; r.VolumeTotalOriginal = lots;
        r.TimeCondition = tif == 0 ? THOST_FTDC_TC_GFD : THOST_FTDC_TC_IOC;
        r.VolumeCondition = tif == 2 ? THOST_FTDC_VC_CV : THOST_FTDC_VC_AV; r.MinVolume = tif == 2 ? lots : 1;
        r.ContingentCondition = THOST_FTDC_CC_Immediately; r.ForceCloseReason = THOST_FTDC_FCC_NotForceClose; r.RequestID = id;
        return x.api->ReqOrderInsert(&r, id);
    });
}
extern "C" int nf_ctp_cancel(void* h, int id, const char* broker, const char* investor, const char* symbol, const char* ref, const char* system, int front, int session) {
    return request(h, [&](Handle& x) {
        CThostFtdcInputOrderActionField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.UserID, x.user.c_str()); copy(r.InstrumentID, symbol); copy(r.ExchangeID, "CFFEX");
        copy(r.OrderRef, ref); copy(r.OrderSysID, system); r.FrontID = front; r.SessionID = session; r.RequestID = id; r.OrderActionRef = id; r.ActionFlag = THOST_FTDC_AF_Delete;
        return x.api->ReqOrderAction(&r, id);
    });
}
