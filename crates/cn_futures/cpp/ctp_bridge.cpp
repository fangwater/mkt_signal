#include "ctp_bridge.h"
#include "ThostFtdcTraderApi.h"
#include <cstdio>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace {
static_assert(std::is_standard_layout_v<NfEvent>);
static_assert(sizeof(TThostFtdcErrorMsgType) <= sizeof(NfHotEvent::message));
static_assert(sizeof(NfHotEvent) == 328);
static_assert(sizeof(NfEvent) == 928);
static_assert(offsetof(NfEvent, hot) == 0);
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
        e.hot.request=request; e.hot.last=last;
        if(info){e.hot.error=info->ErrorID;field(e.hot.message,info->ErrorMsg);}
        callback(context,nullptr,&e);
    }
    void send(NfHotEvent& e, CThostFtdcRspInfoField* info = nullptr, int request = 0, bool last = true) {
        e.request=request; e.last=last;
        if(info){e.error=info->ErrorID;field(e.message,info->ErrorMsg);}
        callback(context,&e,nullptr);
    }
    static void order(NfHotEvent& e, CThostFtdcOrderField* p) {
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
    static void trade(NfHotEvent& e, CThostFtdcTradeField* p) {
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
    void OnFrontConnected() override { NfEvent e{}; e.hot.kind = 1; send(e); }
    void OnFrontDisconnected(int reason) override { NfEvent e{}; e.hot.kind = 2; e.hot.status = reason; send(e); }
    void OnRspAuthenticate(CThostFtdcRspAuthenticateField*, CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.hot.kind = 3; send(e, i, r, l); }
    void OnRspUserLogin(CThostFtdcRspUserLoginField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 4;
        if (p) { e.hot.present = 1; e.hot.front = p->FrontID; e.hot.session = p->SessionID; field(e.max_order_ref, p->MaxOrderRef); field(e.hot.trading_day, p->TradingDay); }
        send(e, i, r, l);
    }
    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField*, CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.hot.kind = 5; send(e, i, r, l); }
    void OnRspQryInstrument(CThostFtdcInstrumentField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 6;
        if (p) { e.hot.present = 1; field(e.hot.symbol, p->InstrumentID); field(e.hot.exchange, p->ExchangeID); e.price_tick = p->PriceTick; e.contract_multiple = p->VolumeMultiple; e.expiry = std::atoi(p->ExpireDate); e.market_minimum = p->MinMarketOrderVolume; e.market_maximum = p->MaxMarketOrderVolume; field(e.product, p->ProductID); e.active = p->IsTrading; e.product_class = p->ProductClass; e.minimum = p->MinLimitOrderVolume; e.maximum = p->MaxLimitOrderVolume; }
        send(e, i, r, l);
    }
    void OnRspQryTradingAccount(CThostFtdcTradingAccountField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 7;
        if (p) { e.hot.present = 1; e.available = p->Available; e.account_values[0] = p->PreMortgage; e.account_values[1] = p->PreCredit; e.account_values[2] = p->PreDeposit; e.account_values[3] = p->PreBalance; e.account_values[4] = p->PreMargin; e.account_values[5] = p->InterestBase; e.account_values[6] = p->Interest; e.account_values[7] = p->Deposit; e.account_values[8] = p->Withdraw; e.account_values[9] = p->FrozenMargin; e.account_values[10] = p->FrozenCash; e.account_values[11] = p->FrozenCommission; e.account_values[12] = p->CurrMargin; e.account_values[13] = p->CashIn; e.account_values[14] = p->Commission; e.account_values[15] = p->CloseProfit; e.account_values[16] = p->PositionProfit; e.account_values[17] = p->Balance; e.account_values[18] = p->Available; e.account_values[19] = p->WithdrawQuota; e.account_values[20] = p->Reserve; e.account_values[21] = p->Credit; e.account_values[22] = p->Mortgage; e.account_values[23] = p->ExchangeMargin; e.account_values[24] = p->DeliveryMargin; e.account_values[25] = p->ExchangeDeliveryMargin; e.account_values[26] = p->ReserveBalance; e.account_values[27] = p->PreFundMortgageIn; e.account_values[28] = p->PreFundMortgageOut; e.account_values[29] = p->FundMortgageIn; e.account_values[30] = p->FundMortgageOut; e.account_values[31] = p->FundMortgageAvailable; e.account_values[32] = p->MortgageableFund; e.account_values[33] = p->SpecProductMargin; e.account_values[34] = p->SpecProductFrozenMargin; e.account_values[35] = p->SpecProductCommission; e.account_values[36] = p->SpecProductFrozenCommission; e.account_values[37] = p->SpecProductPositionProfit; e.account_values[38] = p->SpecProductCloseProfit; e.account_values[39] = p->SpecProductPositionProfitByAlg; e.account_values[40] = p->SpecProductExchangeMargin; e.account_values[41] = p->FrozenSwap; e.account_values[42] = p->RemainSwap; e.account_values[43] = p->OptionValue;  field(e.hot.investor, p->AccountID); field(e.hot.trading_day, p->TradingDay); field(e.hot.exchange, p->CurrencyID); }
        send(e, i, r, l);
    }
    void OnRspQrySettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 20;
        if (p) { e.hot.present = 1; field(e.hot.trading_day, p->ConfirmDate); field(e.hot.investor, p->InvestorID); }
        send(e, i, r, l);
    }
    void OnRspQryInstrumentCommissionRate(CThostFtdcInstrumentCommissionRateField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 17;
        if (p) { e.hot.present = 1; field(e.hot.symbol,p->InstrumentID); field(e.hot.exchange,p->ExchangeID); field(e.hot.investor,p->InvestorID); e.investor_range=p->InvestorRange;
            e.commission_rates[0]=p->OpenRatioByMoney; e.commission_rates[1]=p->OpenRatioByVolume;
            e.commission_rates[2]=p->CloseRatioByMoney; e.commission_rates[3]=p->CloseRatioByVolume;
            e.commission_rates[4]=p->CloseTodayRatioByMoney; e.commission_rates[5]=p->CloseTodayRatioByVolume; }
        send(e,i,r,l);
    }
    void OnRspQryInstrumentMarginRate(CThostFtdcInstrumentMarginRateField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 18;
        if (p) { e.hot.present=1; field(e.hot.symbol,p->InstrumentID); field(e.hot.exchange,p->ExchangeID); field(e.hot.investor,p->InvestorID); e.investor_range=p->InvestorRange; e.hot.hedge=p->HedgeFlag; e.relative=p->IsRelative;
            e.margin_rates[0]=p->LongMarginRatioByMoney; e.margin_rates[1]=p->LongMarginRatioByVolume;
            e.margin_rates[2]=p->ShortMarginRatioByMoney; e.margin_rates[3]=p->ShortMarginRatioByVolume; }
        send(e,i,r,l);
    }
    void OnRspQryExchangeMarginRate(CThostFtdcExchangeMarginRateField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind=19;
        if (p) { e.hot.present=1; field(e.hot.symbol,p->InstrumentID); field(e.hot.exchange,p->ExchangeID); e.hot.hedge=p->HedgeFlag;
            e.margin_rates[0]=p->LongMarginRatioByMoney; e.margin_rates[1]=p->LongMarginRatioByVolume;
            e.margin_rates[2]=p->ShortMarginRatioByMoney; e.margin_rates[3]=p->ShortMarginRatioByVolume; }
        send(e,i,r,l);
    }
    void OnRspQryInvestorPosition(CThostFtdcInvestorPositionField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfEvent e{}; e.hot.kind = 8;
        if (p) { e.hot.present = 1; field(e.hot.symbol, p->InstrumentID); field(e.hot.exchange, p->ExchangeID); field(e.hot.investor, p->InvestorID); field(e.hot.trading_day, p->TradingDay); e.hot.volume = p->Position; e.today = p->TodayPosition; e.yesterday = p->YdPosition; e.hot.hedge = p->HedgeFlag; e.position_direction = p->PosiDirection; e.position_date = p->PositionDate; }
        send(e, i, r, l);
    }
    void OnRtnOrder(CThostFtdcOrderField* p) override { NfHotEvent e{}; e.kind = 9; order(e, p); send(e); }
    void OnRtnTrade(CThostFtdcTradeField* p) override { NfHotEvent e{}; e.kind = 10; trade(e, p); send(e); }
    void OnRspOrderInsert(CThostFtdcInputOrderField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfHotEvent e{}; e.kind = 11;
        if (p) { e.present = 1; field(e.order_ref, p->OrderRef); field(e.symbol, p->InstrumentID); }
        send(e, i, r, l);
    }
    void OnErrRtnOrderInsert(CThostFtdcInputOrderField* p, CThostFtdcRspInfoField* i) override { OnRspOrderInsert(p, i, p ? p->RequestID : 0, true); }
    void OnRspOrderAction(CThostFtdcInputOrderActionField* p, CThostFtdcRspInfoField* i, int r, bool l) override {
        NfHotEvent e{}; e.kind = 12;
        if (p) { e.present = 1; e.front = p->FrontID; e.session = p->SessionID; field(e.order_ref, p->OrderRef); field(e.system_id, p->OrderSysID); }
        send(e, i, r, l);
    }
    void OnErrRtnOrderAction(CThostFtdcOrderActionField* p, CThostFtdcRspInfoField* i) override {
        NfHotEvent e{}; e.kind = 12;
        if (p) { e.present = 1; e.front = p->FrontID; e.session = p->SessionID; field(e.order_ref, p->OrderRef); field(e.system_id, p->OrderSysID); }
        send(e, i, p ? p->RequestID : 0);
    }
    void OnRspQryOrder(CThostFtdcOrderField* p, CThostFtdcRspInfoField* i, int r, bool l) override { NfHotEvent e{}; e.kind = 13; order(e, p); send(e, i, r, l); }
    void OnRspQryTrade(CThostFtdcTradeField* p, CThostFtdcRspInfoField* i, int r, bool l) override { NfHotEvent e{}; e.kind = 14; trade(e, p); send(e, i, r, l); }
    void OnRtnInstrumentStatus(CThostFtdcInstrumentStatusField* p) override {
        NfEvent e{}; e.hot.kind = 15;
        if (p) { e.hot.present = 1; field(e.hot.symbol, p->InstrumentID); field(e.hot.exchange, p->ExchangeID); e.hot.status = p->InstrumentStatus; }
        send(e);
    }
    void OnRspError(CThostFtdcRspInfoField* i, int r, bool l) override { NfEvent e{}; e.hot.kind = 16; send(e, i, r, l); }
};
struct ContractRequests { CThostFtdcInputOrderField insert{}; CThostFtdcInputOrderActionField cancel{}; };
struct Handle {
    Library library; Spi spi; CThostFtdcTraderApi* api = nullptr; std::string front, user; std::vector<std::string> fronts; std::vector<ContractRequests> contracts;
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
extern "C" size_t nf_ctp_hot_event_size() { return sizeof(NfHotEvent); }
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
    return request(h, [&](Handle& x) { x.front = front; x.api->SubscribePrivateTopic(THOST_TERT_RESUME); x.api->SubscribePublicTopic(THOST_TERT_RESTART); size_t offset=0; for (;;) { auto end=x.front.find(',',offset); x.fronts.push_back(x.front.substr(offset,end==std::string::npos?end:end-offset)); if(end==std::string::npos) break; offset=end+1; } for(auto& address:x.fronts) x.api->RegisterFront(address.data()); x.api->Init(); return 0; });
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
extern "C" int nf_ctp_query(void* h, int kind, int id, const char* broker, const char* investor, const char* exchange, const char* symbol) {
    return request(h, [&](Handle& x) {
        switch (kind) {
        case 6: { CThostFtdcQryInstrumentField r{}; return x.api->ReqQryInstrument(&r, id); }
        case 7: { CThostFtdcQryTradingAccountField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); copy(r.CurrencyID, "CNY"); return x.api->ReqQryTradingAccount(&r, id); }
        case 8: { CThostFtdcQryInvestorPositionField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); return x.api->ReqQryInvestorPosition(&r, id); }
        case 13: { CThostFtdcQryOrderField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); return x.api->ReqQryOrder(&r, id); }
        case 14: { CThostFtdcQryTradeField r{}; copy(r.BrokerID, broker); copy(r.InvestorID, investor); return x.api->ReqQryTrade(&r, id); }
        case 17: { CThostFtdcQryInstrumentCommissionRateField r{}; copy(r.BrokerID,broker); copy(r.InvestorID,investor); copy(r.ExchangeID,exchange); copy(r.InstrumentID,symbol); return x.api->ReqQryInstrumentCommissionRate(&r,id); }
        case 18: { CThostFtdcQryInstrumentMarginRateField r{}; copy(r.BrokerID,broker); copy(r.InvestorID,investor); copy(r.ExchangeID,exchange); copy(r.InstrumentID,symbol); r.HedgeFlag=THOST_FTDC_HF_Speculation; return x.api->ReqQryInstrumentMarginRate(&r,id); }
        case 19: { CThostFtdcQryExchangeMarginRateField r{}; copy(r.BrokerID,broker); copy(r.ExchangeID,exchange); copy(r.InstrumentID,symbol); r.HedgeFlag=THOST_FTDC_HF_Speculation; return x.api->ReqQryExchangeMarginRate(&r,id); }
        case 20: { CThostFtdcQrySettlementInfoConfirmField r{}; copy(r.BrokerID,broker); copy(r.InvestorID,investor); return x.api->ReqQrySettlementInfoConfirm(&r,id); }
        default: return invalid;
        }
    });
}
extern "C" int nf_ctp_prepare(void* h, uint32_t slot, const char* broker, const char* investor, const char* user, const char* exchange, const char* symbol) {
    return request(h,[&](Handle& x){
        if(slot!=x.contracts.size()) return invalid;
        ContractRequests c{};
        copy(c.insert.BrokerID,broker);copy(c.insert.InvestorID,investor);copy(c.insert.UserID,user);copy(c.insert.ExchangeID,exchange);copy(c.insert.InstrumentID,symbol);
        c.insert.OrderPriceType=THOST_FTDC_OPT_LimitPrice;c.insert.CombHedgeFlag[0]=THOST_FTDC_HF_Speculation;
        c.insert.ContingentCondition=THOST_FTDC_CC_Immediately;c.insert.ForceCloseReason=THOST_FTDC_FCC_NotForceClose;
        copy(c.cancel.BrokerID,broker);copy(c.cancel.InvestorID,investor);copy(c.cancel.UserID,user);copy(c.cancel.ExchangeID,exchange);copy(c.cancel.InstrumentID,symbol);
        c.cancel.ActionFlag=THOST_FTDC_AF_Delete;x.contracts.push_back(c);return 0;
    });
}
namespace {
void reference(char (&out)[13],uint64_t value) {
    if(value>999999999999ULL)throw std::runtime_error("CTP reference exceeds SDK capacity");
    auto result=std::to_chars(out,out+12,value);
    if(result.ec!=std::errc{})throw std::runtime_error("CTP reference formatting failed");
    *result.ptr=0;
}
}
extern "C" int nf_ctp_insert(void* h,uint32_t slot,int id,uint64_t ref,int direction,int offset,int tif,int lots,double price) {
    return request(h,[&](Handle& x){
        if(slot>=x.contracts.size() || lots<=0 || !std::isfinite(price) || price<=0 || (direction!=48 && direction!=49) || (offset!=48 && offset!=49 && offset!=51 && offset!=52) || tif<0 || tif>2)return invalid;
        // Copy an immutable template: never assume the SDK leaves a mutable input unchanged.
        auto r=x.contracts[slot].insert;reference(r.OrderRef,ref);r.RequestID=id;
        r.Direction=static_cast<char>(direction);r.CombOffsetFlag[0]=static_cast<char>(offset);
        r.LimitPrice=price;r.VolumeTotalOriginal=lots;
        r.TimeCondition=tif==0?THOST_FTDC_TC_GFD:THOST_FTDC_TC_IOC;
        r.VolumeCondition=tif==2?THOST_FTDC_VC_CV:THOST_FTDC_VC_AV;r.MinVolume=tif==2?lots:1;
        return x.api->ReqOrderInsert(&r,id);
    });
}
extern "C" int nf_ctp_cancel(void* h,uint32_t slot,int id,uint64_t ref,const uint8_t* system,size_t length,int front,int session) {
    return request(h,[&](Handle& x){
        if(slot>=x.contracts.size() || length>=sizeof(CThostFtdcInputOrderActionField::OrderSysID) || (length && !system))return invalid;
        auto r=x.contracts[slot].cancel;reference(r.OrderRef,ref);
        if(length)std::memcpy(r.OrderSysID,system,length);
        r.OrderSysID[length]=0;
        r.FrontID=front;r.SessionID=session;r.RequestID=id;r.OrderActionRef=id;
        return x.api->ReqOrderAction(&r,id);
    });
}
