#include "yida_bridge.h"
#include "ydApi.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace {
constexpr int invalid = -9901;
constexpr int transport_unavailable = 9902; // Certain local rejection: no SDK send attempted.
constexpr int local_invalid = 9904; // Certain pre-send argument/permission rejection.
constexpr int market_price_unavailable = 9903; // Missing native daily price-limit data; nothing sent.
// Event IDs are bridge-owned, vendor integer constants remain unchanged in the fields.
enum { Connected=1, Disconnected, ReadyLogin, Login, Init, Begin, Instrument, Account,
       Position, Order, Trade, End, FailedCancel, Invalidated, Segment, Fatal, Transport, Connection };
static_assert(std::is_standard_layout_v<CnYidaEvent>);
// Pin the production headers as well as the loaded library. In 1.502.55 the
// long instrument ID moved and account/position fields shifted in place.
static_assert(sizeof(YDInstrument::InstrumentID)==64, "1.502.55.35 requires the full instrument ID");
static_assert(sizeof(YDInstrument::ShortInstrumentID)==32, "1.502.55.35 legacy short ID layout mismatch");
static_assert(sizeof(YDInstrument)==544 && offsetof(YDInstrument,InstrumentID)==112, "1.502.55.35 instrument layout mismatch");
static_assert(sizeof(YDExtendedAccount)==152 && offsetof(YDExtendedAccount,Available)==64, "1.502.55.35 account layout mismatch");
static_assert(sizeof(YDExtendedPosition)==168 && offsetof(YDExtendedPosition,MarginPerLot2)==104, "1.502.55.35 position layout mismatch");
// This new virtual slot is part of the ABI. The query locks the SDK trading core,
// so do not call it in the submission/callback path (vendor guide, section 4.3).
static_assert(std::is_same_v<decltype(&YDExtendedApi::getMaxWithdrawable),double(YDExtendedApi::*)(const YDAccount*)>, "1.502.55.35 API signature mismatch");
void msg(char* out, size_t cap, const char* value) { if (out && cap) std::snprintf(out, cap, "%s", value); }
template<size_t N, size_t M> void field(char (&out)[N], const char (&in)[M]) {
    const auto n = strnlen(in, M);
    if (n >= M || n >= N) throw std::runtime_error("invalid Yida identifier length");
    std::memcpy(out, in, n + 1);
}
template<size_t N> void text(char (&out)[N], const char* in) {
    if (!in) return;
    if (std::strlen(in) >= N) throw std::runtime_error("Yida original ID too long");
    std::strcpy(out, in);
}
struct Library {
    void* handle;
    bool retain = false;
    explicit Library(const char* path): handle(dlopen(path, RTLD_NOW | RTLD_LOCAL)) { if (!handle) throw std::runtime_error("cannot load Yida library/dependencies"); }
    ~Library() { if (handle && !retain) dlclose(handle); }
    template<typename T> T symbol(const char* name) { auto p = dlsym(handle, name); if (!p) throw std::runtime_error("missing native Yida symbol"); return reinterpret_cast<T>(p); }
    void check() { const char* v = symbol<const char*(*)()>("getYDVersion")(); if (!v || std::strcmp(v, "1.502.55.35")) throw std::runtime_error("Yida SDK must match reference headers 1.502.55.35"); }
};
template<typename T> struct ResultDeleter { void operator()(YDQueryResult<T>* p) const { if (p) p->destroy(); } };
template<typename T> using Query = std::unique_ptr<YDQueryResult<T>, ResultDeleter<T>>;

class Bridge final: public YDListener, public YDExtendedListener {
public:
    Library library;
    YDExtendedApi* api = nullptr;
    CnYidaCallback callback;
    void* context;
    std::atomic<bool> destroying{false}, initialized{false}, logged_in{false};
    std::atomic<bool> tcp_connected{false}, xtcp_connected{false}, caught_up{false};
    std::atomic<int> trading_port{0};
    const int protocol; // TCP=0, UDP=1, XTCP=2.
    std::mutex destroy_mutex;
    std::condition_variable destroyed_cv;
    bool destroyed = false;
    int max_ref = -1;
    const int order_group;
    std::vector<std::pair<std::string,std::string>> instrument_allowlist;
    bool allowed(const YDInstrument* i) const {
        if(!i || !i->m_pExchange) return false;
        for(const auto& entry:instrument_allowlist) if(entry.first==i->m_pExchange->ExchangeID && entry.second==i->InstrumentID) return true;
        return false;
    }
    Bridge(const char* lib, const char* config, int transport, int group, CnYidaCallback cb, void* ctx): library(lib), callback(cb), context(ctx), protocol(transport), order_group(group) {
        if (group<1 || group>255) throw std::runtime_error("nonzero order group required");
        if (protocol<0 || protocol>2) throw std::runtime_error("invalid native trading protocol");
        library.check(); api = library.symbol<YDExtendedApi*(*)(const char*)>("makeYDExtendedApi")(config);
        if (!api) throw std::runtime_error("makeYDExtendedApi failed");
        // Keep code mapped even after asynchronous API destruction: vendor threads may still
        // return through their shared-library stack after notifyAfterApiDestroy returns.
        library.retain = true;
    }
    void send(CnYidaEvent e) noexcept { callback(context, &e); }
    void fatal() noexcept { CnYidaEvent e{}; e.kind=Fatal; e.error=invalid; send(e); }
    bool can_send() const {
        return !destroying && tcp_connected && initialized && logged_in && caught_up &&
            (protocol==0 || (trading_port>0 && (protocol!=2 || xtcp_connected)));
    }
    void transport_event() {
        CnYidaEvent e{}; e.kind=Transport; e.status=protocol; e.flag=can_send(); e.minimum=trading_port; send(e);
    }
    void read_trading_port() {
        trading_port=0;
        if (protocol==0) return;
        const auto* param=api->getSystemParamByName(protocol==1?"UDPTradingPort":"XTCPTradingPort","System");
        if (!param || strnlen(param->Value,sizeof(param->Value))==sizeof(param->Value)) return;
        char* end=nullptr;
        const long value=std::strtol(param->Value,&end,10);
        if (end!=param->Value && *end=='\0' && value>0 && value<=65535) trading_port=static_cast<int>(value);
    }
    template<typename F> void guarded(F f) noexcept { try { f(); } catch (...) { fatal(); } }
    void base(CnYidaEvent& e, const YDInstrument* i) {
        if (!i || !i->m_pExchange) throw std::runtime_error("missing Yida instrument metadata");
        field(e.symbol, i->InstrumentID); field(e.exchange, i->m_pExchange->ExchangeID);
        e.product_class=i->ProductClass; e.day=api->getTradingDay();
        e.expiry=i->ExpireDate; e.contract_multiple=i->Multiple; e.options_type=i->OptionsType;
        e.strike_price=i->StrikePrice;
        if(i->m_pMarketData) e.upper_limit_price=i->m_pMarketData->UpperLimitPrice;
        if(i->ProductClass==YD_PC_Options && i->m_pUnderlyingInstrument) {
            field(e.underlying,i->m_pUnderlyingInstrument->InstrumentID);
            if(i->m_pUnderlyingInstrument->m_pExchange) field(e.underlying_exchange,i->m_pUnderlyingInstrument->m_pExchange->ExchangeID);
        }
    }
    bool mine(const YDAccount* a) { return a && a == api->getMyAccount(); }
    void original(CnYidaEvent& e, const YDExchange* x, int order_id, int trade_id=0) {
        if (order_id) text(e.original_order_id, api->getIDFromExchange(x, YD_IDT_NormalOrderSysID, order_id));
        if (trade_id) text(e.original_trade_id, api->getIDFromExchange(x, YD_IDT_TradeID, trade_id));
    }
    void order(const YDOrder* o, const YDInstrument* i, bool replay=false) {
        if (!o) throw std::runtime_error("missing order");
        if (o->YDOrderFlag!=YD_YOF_Normal) { unsupported(o); return; }
        CnYidaEvent e{}; e.kind=Order; e.replay=replay; base(e, i);
        e.reference=o->OrderRef; e.system_id=o->LongOrderSysID; e.direction=o->Direction;
        e.legacy_system_id=o->OrderSysID; e.order_group=o->OrderGroupID;
        e.offset=o->OffsetFlag; e.hedge=o->HedgeFlag; e.status=o->OrderStatus;
        e.lots=o->OrderVolume; e.traded=o->TradeVolume; e.price=o->Price;
        e.error=o->ErrorNo; e.flag=o->YDOrderFlag; e.native_time=o->InsertTime;
        e.order_type=o->OrderType; e.order_attribute=o->ExchangeOrderAttribute;
        e.selection_type=o->ConnectionSelectionType; e.connection_id=o->ConnectionID; e.real_connection_id=o->RealConnectionID;
        original(e, i->m_pExchange, o->OrderSysID); send(e);
    }
    void unsupported(const YDOrder* o) {
        CnYidaEvent e{}; e.kind=Fatal; e.error=invalid;
        if(o) { e.reference=o->OrderRef; e.system_id=o->LongOrderSysID; e.flag=o->YDOrderFlag;
            e.legacy_system_id=o->OrderSysID; e.order_group=o->OrderGroupID;
            e.direction=o->Direction; e.offset=o->OffsetFlag; e.hedge=o->HedgeFlag;
            e.status=o->OrderStatus; e.lots=o->OrderVolume; e.traded=o->TradeVolume; }
        send(e);
    }
    void trade(const YDTrade* t, const YDInstrument* i, bool replay=false) {
        if (!t) throw std::runtime_error("missing trade");
        CnYidaEvent e{}; e.kind=Trade; e.replay=replay; base(e, i);
        e.reference=t->OrderRef; e.system_id=t->LongOrderSysID; e.trade_id=t->LongTradeID;
        e.legacy_system_id=t->OrderSysID; e.legacy_trade_id=t->TradeID; e.order_group=t->OrderGroupID;
        e.trade_flag=t->YDTradeFlag; e.no_order=t->NoOrderFlag;
        e.direction=t->Direction; e.offset=t->OffsetFlag; e.hedge=t->HedgeFlag;
        e.lots=t->Volume; e.price=t->Price; e.commission=t->Commission; e.native_time=t->TradeTime;
        original(e, i->m_pExchange, t->OrderSysID, t->TradeID); send(e);
    }
    void account(bool replay=false) {
        const auto* a=api->getMyAccount(); const auto* x=api->getExtendedAccount();
        if (!a || !x) throw std::runtime_error("missing trader account");
        CnYidaEvent e{}; e.kind=Account; e.replay=replay; e.day=api->getTradingDay(); e.available=x->usable();
        e.right=a->TradingRight; e.login_count=a->LoginCount; send(e);
    }
    int right(const YDInstrument* i) {
        const auto* a=api->getMyAccount();
        const auto* x=api->getAccountExchangeInfo(i->m_pExchange);
        const auto* p=i->m_pProduct ? api->getAccountProductInfo(i->m_pProduct) : nullptr;
        const auto* n=api->getAccountInstrumentInfo(i);
        if (!a || !x || !p || !n) throw std::runtime_error("incomplete native permissions");
        for(int r:{a->TradingRight,x->TradingRight,p->TradingRight,n->TradingRight})
            if(r<YD_TR_Allow || r>YD_TR_Forbidden) throw std::runtime_error("invalid native permission value");
        return std::max({a->TradingRight, x->TradingRight, p->TradingRight, n->TradingRight});
    }
    void snapshot() {
        // Called on the vendor's caught-up notification thread, never by the business
        // loop concurrently with live state reads. Live interleaving is rejected in Rust.
        CnYidaEvent begin{}; begin.kind=Begin; begin.day=api->getTradingDay(); begin.reference=max_ref; send(begin);
        account(true);
        for(int k=0;k<api->getExchangeCount();++k) {
            const auto* x=api->getExchange(k);
            if(x && (!std::strcmp(x->ExchangeID,"SHFE") || !std::strcmp(x->ExchangeID,"INE"))) connections(x);
        }
        for (const auto& entry:instrument_allowlist) {
            const auto* i=api->getInstrumentByID(entry.second.c_str());
            if (!i || !i->m_pExchange) continue; // Rust rejects an incomplete configured catalog.
            if (entry.first!=i->m_pExchange->ExchangeID) throw std::runtime_error("configured catalog venue mismatch");
            CnYidaEvent e{}; e.kind=Instrument; base(e,i); e.tick=i->Tick;
            e.minimum=i->MinLimitOrderVolume; e.maximum=i->MaxLimitOrderVolume; e.expiry=i->ExpireDate;
            e.market_minimum=i->MinMarketOrderVolume; e.market_maximum=i->MaxMarketOrderVolume;
            e.right=right(i); send(e);
        }
        YDExtendedPositionFilter pf{}; pf.PositionDate=pf.PositionDirection=pf.HedgeFlag=-1;
        Query<YDExtendedPosition> ps(api->findExtendedPositions(&pf));
        if (!ps) throw std::runtime_error("position query failed");
        for (int k=0;k<ps->getCount();++k) {
            const auto* p=ps->get(k);
            if (!p || !p->m_pAccountInstrumentInfo) throw std::runtime_error("invalid position row");
            if (!mine(p->m_pAccountInstrumentInfo->m_pAccount)) continue;
            if (!p->Position && !p->CloseFrozen && !p->OpenFrozen && !p->ExecFrozen) continue;
            CnYidaEvent e{}; e.kind=Position; base(e,p->m_pAccountInstrumentInfo->m_pInstrument);
            if (p->ExecFrozen) throw std::runtime_error("unsupported execution frozen position");
            e.date=p->PositionDate; e.position_direction=p->PositionDirection; e.hedge=p->HedgeFlag;
            e.lots=p->Position; e.frozen=p->CloseFrozen; send(e);
        }
        YDOrderFilter of{}; of.StartTime=of.EndTime=-1; of.YDOrderFlags=-1;
        Query<YDExtendedOrder> os(api->findOrders(&of));
        if (!os) throw std::runtime_error("order query failed");
        for (int k=0;k<os->getCount();++k) {
            const auto* o=os->get(k); if (!o) throw std::runtime_error("invalid order row");
            if (mine(o->m_pAccount)) order(o,o->m_pInstrument,true);
        }
        YDTradeFilter tf{}; tf.StartTime=tf.EndTime=-1;
        Query<YDExtendedTrade> ts(api->findTrades(&tf));
        if (!ts) throw std::runtime_error("trade query failed");
        for (int k=0;k<ts->getCount();++k) {
            const auto* t=ts->get(k); if (!t) throw std::runtime_error("invalid trade row");
            if (mine(t->m_pAccount)) trade(t,t->m_pInstrument,true);
        }
        CnYidaEvent end{}; end.kind=End; end.day=begin.day; send(end);
    }
    void start_destroy() { if (!destroying.exchange(true)) api->startDestroy(); }
    void notifyAfterApiDestroy() override { std::lock_guard<std::mutex> lock(destroy_mutex); destroyed=true; destroyed_cv.notify_all(); }
    void notifyEvent(int event) override { guarded([&] {
        if (event==YD_AE_ServerRestarted) { logged_in=false; caught_up=false; fatal(); start_destroy(); return; }
        CnYidaEvent e{};
        if (event==YD_AE_XTCPTradeConnected || event==YD_AE_XTCPTradeDisconnected) {
            xtcp_connected=event==YD_AE_XTCPTradeConnected; transport_event(); return;
        }
        if (event==YD_AE_TCPTradeConnected) { tcp_connected=true; e.kind=Connected; }
        else if (event==YD_AE_TCPTradeDisconnected || event==YD_AE_ServerSwitched) {
            // The SDK can retain its independent XTCP socket while reconnecting
            // the TCP report channel, without another XTCP-connected callback.
            tcp_connected=false; logged_in=false; caught_up=false;
            if (event==YD_AE_ServerSwitched) xtcp_connected=false;
            e.kind=Disconnected;
        }
        else return;
        e.status=event; send(e); transport_event();
    }); }
    void notifyReadyForLogin(bool failed) override { CnYidaEvent e{}; e.kind=ReadyLogin; e.error=failed?1:0; send(e); }
    void notifyLogin(int err,int ref,bool monitor) override { guarded([&] {
        max_ref=-1; logged_in=err==0 && !monitor;
        if (logged_in) tcp_connected=true;
        CnYidaEvent e{}; e.kind=Login; e.error=err; e.reference=ref; e.flag=monitor; e.day=api->getTradingDay(); send(e);
    }); }
    void notifyGroupMaxOrderRef(const int refs[]) override { guarded([&] {
        if (!refs || refs[order_group]<0) throw std::runtime_error("invalid group maximum reference");
        max_ref=refs[order_group];
    }); }
    void notifyFinishInit() override { initialized=true; CnYidaEvent e{}; e.kind=Init; send(e); }
    void notifyCaughtUp() override { guarded([&] {
        if (!initialized || !logged_in || max_ref<0) throw std::runtime_error("caught up before initialization/login");
        read_trading_port(); snapshot(); caught_up=true; transport_event();
    }); }
    void notifyOrder(const YDOrder* o,const YDInstrument* i,const YDAccount* a) override { guarded([&] { if (mine(a)) order(o,i); }); }
    void notifyTrade(const YDTrade* t,const YDInstrument* i,const YDAccount* a) override { guarded([&] { if (mine(a)) trade(t,i); }); }
    void notifyFailedCancelOrder(const YDFailedCancelOrder* o,const YDExchange* x,const YDAccount* a) override { guarded([&] {
        if (!mine(a) || !o || !x) return;
        CnYidaEvent e{}; e.kind=FailedCancel; field(e.exchange,x->ExchangeID); e.system_id=o->LongOrderSysID;
        e.legacy_system_id=o->OrderSysID; e.order_group=o->OrderGroupID; e.reference=o->OrderRef; e.error=o->ErrorNo; send(e);
    }); }
    void connections(const YDExchange* x) {
        if (!x || x->ConnectionCount<0 || x->ConnectionCount>64 || (x->ConnectionCount && !x->ConnectionInfos)) throw std::runtime_error("invalid native seat catalog");
        const auto* a=api->getAccountExchangeInfo(x);
        if (!a) throw std::runtime_error("missing account seat permissions");
        for(int k=0;k<x->ConnectionCount;++k) {
            const auto& c=x->ConnectionInfos[k]; CnYidaEvent e{}; e.kind=Connection;
            field(e.exchange,x->ExchangeID); e.connection_id=k; e.connection_count=x->ConnectionCount;
            e.connection_status=c.ConnectionStatus; e.public_connection=x->IsPublicConnectionID[k]; e.dedicated_connection=a->IsDedicatedConnectionID[k];
            field(e.connection_info,c.Info); field(e.insert_flow,c.InsertFlowControl); field(e.cancel_flow,c.CancelFlowControl); send(e);
        }
    }
    void notifyExchangeConnectionInfo(const YDExchangeConnectionInfo* c) override { guarded([&] {
        if (!c) throw std::runtime_error("missing seat update");
        for(int k=0;k<api->getExchangeCount();++k) { const auto* x=api->getExchange(k); if(x && x->ExchangeRef==c->ExchangeRef) { connections(x); return; } }
        throw std::runtime_error("seat update for unknown exchange");
    }); }
    int route(const YDExchange* x,int selection,int connection) {
        if(selection<YD_CS_Any || selection>YD_CS_Packed) return local_invalid;
        if(selection==YD_CS_Any || selection==YD_CS_Packed) return connection==0?0:local_invalid;
        const auto* a=api->getAccountExchangeInfo(x);
        if(!a || connection<0 || connection>=x->ConnectionCount || connection>=64 || !x->ConnectionInfos ||
            (!x->IsPublicConnectionID[connection] && !a->IsDedicatedConnectionID[connection])) return local_invalid;
        if(selection==YD_CS_Fixed && x->ConnectionInfos[connection].ConnectionStatus!=YD_ECS_CONNECTED) return local_invalid;
        return 0;
    }
    int make_order(const CnYidaOrderRequest& r,YDInputOrder& o,const YDInstrument*& i) {
        if(!can_send()) return transport_unavailable;
        i=api->getInstrumentByID(r.symbol);
        if(!allowed(i)) return local_invalid;
        if(!i || !i->m_pExchange || std::strcmp(i->m_pExchange->ExchangeID,r.exchange) || (i->ProductClass!=YD_PC_Futures && i->ProductClass!=YD_PC_Options) || !i->m_pExchange->UseTodayPosition || r.reference<=0) return local_invalid;
        if(std::strcmp(r.exchange,"SHFE") && std::strcmp(r.exchange,"INE")) return local_invalid;
        if(r.offset!=YD_OF_Open && r.offset!=YD_OF_CloseToday && r.offset!=YD_OF_CloseYesterday) return local_invalid;
        if(r.tif!=YD_ODT_Limit && r.tif!=YD_ODT_FAK && r.tif!=YD_ODT_FOK) return local_invalid;
        if(r.market!=0 && r.market!=1) return local_invalid;
        if(r.direction!=YD_D_Buy && r.direction!=YD_D_Sell) return local_invalid;
        const auto* a=api->getMyAccount(); if(!a || a->LoginCount!=1) return local_invalid;
        const int permission=right(i);
        if(permission==YD_TR_Forbidden || (permission==YD_TR_CloseOnly && r.offset==YD_OF_Open) || i->ExpireDate<api->getTradingDay()) return 10;
        int code=route(i->m_pExchange,r.selection,r.connection); if(code) return code;
        o.Direction=r.direction; o.OffsetFlag=r.offset; o.HedgeFlag=YD_HF_Speculation;
        o.OrderRef=r.reference; o.OrderGroupID=order_group; o.GroupOrderRefControl=YD_GORF_Increase;
        o.ConnectionSelectionType=r.selection; o.ConnectionID=r.connection;
        o.Price=r.price; o.OrderVolume=r.lots; o.OrderType=r.tif; o.YDOrderFlag=YD_YOF_Normal;
        if(r.market) {
            o.ExchangeOrderAttribute=!std::strcmp(r.exchange,"SHFE")?YD_EOA_SHFE_MarketPrice:YD_EOA_INE_MarketPrice;
            const auto* m=i->m_pMarketData; if(!m) return market_price_unavailable;
            o.Price=r.direction==YD_D_Buy?m->UpperLimitPrice:m->LowerLimitPrice;
            if(!std::isfinite(o.Price) || o.Price<=0) return market_price_unavailable;
            // The core reserved option-buyer premium using its copied daily upper limit.
            // Reject a raised native ceiling until a fresh full snapshot funds the new bound.
            if(i->ProductClass==YD_PC_Options && r.direction==YD_D_Buy &&
                (!std::isfinite(r.option_market_buy_ceiling) || r.option_market_buy_ceiling<=0 || o.Price>r.option_market_buy_ceiling)) return local_invalid;
        }
        if(!api->checkOrder(&o,i)) return o.ErrorNo>0?o.ErrorNo:local_invalid;
        return o.OrderRef==r.reference && o.OrderGroupID==order_group?0:local_invalid;
    }
    int make_cancel(const CnYidaCancelRequest& r,YDCancelOrder& o,const YDExchange*& x) {
        if(!can_send()) return transport_unavailable;
        x=api->getExchangeByID(r.exchange); if(!x) return local_invalid;
        const int code=route(x,r.selection,r.connection); if(code) return code;
        if(r.system_id>0) o.LongOrderSysID=r.system_id;
        else {
            if(r.group<=0 || r.group>255 || r.reference<=0) return local_invalid;
            o.OrderGroupID=r.group; o.OrderRef=r.reference;
        }
        o.ConnectionSelectionType=r.selection; o.ConnectionID=r.connection; o.YDOrderFlag=YD_YOF_Normal;
        return 0;
    }
    void invalidated() { CnYidaEvent e{}; e.kind=Invalidated; send(e); }
    void notifyAccount(const YDAccount*) override { invalidated(); }
    void notifyAccountExchangeInfo(const YDAccountExchangeInfo*) override { invalidated(); }
    void notifyAccountProductInfo(const YDAccountProductInfo*) override { invalidated(); }
    void notifyAccountInstrumentInfo(const YDAccountInstrumentInfo*) override { invalidated(); }
    void notifyAccountMarginModelInfo(const YDAccountMarginModelInfo*) override { invalidated(); }
    void notifyUpdateMarginRate(const YDUpdateMarginRate*) override { invalidated(); }
    void notifyAccountLoginInfo(const YDAccountLoginInfo*) override { invalidated(); }
    void notifyMissingOrder(const YDMissingOrder* o) override { unsupported(o); }
    void notifyCombPositionOrder(const YDOrder* o,const YDCombPositionDef*,const YDAccount*) override { unsupported(o); }
    void notifyOptionExecTogetherOrder(const YDOrder* o,const YDInstrument*,const YDInstrument*,const YDAccount*) override { unsupported(o); }
    void notifyCombPosition(const YDCombPosition*,const YDCombPositionDef*,const YDAccount*) override { fatal(); }
    void notifyQuote(const YDQuote*,const YDInstrument*,const YDAccount*) override { fatal(); }
    void notifySpotPosition(const YDInstrument*,const YDAccount*,int) override { fatal(); }
    void notifyResponse(int error,int type,int request) override {
        if(error) { CnYidaEvent e{}; e.kind=Fatal; e.error=error; e.status=type; e.reference=request; send(e); }
    }
    void notifyExtendedAccount(const YDExtendedAccount*) override { guarded([&] { if (initialized && logged_in && !destroying) account(); }); }
    void notifyTradingSegment(const YDExchange* x,int time) override { guarded([&] { CnYidaEvent e{}; e.kind=Segment; if(x) field(e.exchange,x->ExchangeID); e.native_time=time; send(e); }); }
};
Bridge* get(void* p) { auto* b=static_cast<Bridge*>(p); if(!b || b->destroying) throw std::runtime_error("API unavailable"); return b; }
}
extern "C" {
size_t cn_yida_event_size() { return sizeof(CnYidaEvent); }
size_t cn_yida_order_request_size() { return sizeof(CnYidaOrderRequest); }
size_t cn_yida_cancel_request_size() { return sizeof(CnYidaCancelRequest); }
int cn_yida_version(const char* path,char* out,size_t cap) { try { Library l(path); l.check(); msg(out,cap,l.symbol<const char*(*)()>("getYDVersion")()); return 0; } catch(const std::exception& e) { msg(out,cap,e.what()); return invalid; } }
void* cn_yida_create(const char* lib,const char* config,int protocol,int group,CnYidaCallback cb,void* ctx,char* error,size_t cap) {
    try { return new Bridge(lib,config,protocol,group,cb,ctx); } catch(const std::exception& e) { msg(error,cap,e.what()); return nullptr; } catch(...) { msg(error,cap,"Yida create exception"); return nullptr; }
}
int cn_yida_destroy(void* p) {
    auto* b=static_cast<Bridge*>(p); if(!b) return 0;
    try {
        b->start_destroy(); std::unique_lock<std::mutex> lock(b->destroy_mutex);
        if (!b->destroyed_cv.wait_for(lock,std::chrono::seconds(5),[&]{return b->destroyed;})) return invalid;
        lock.unlock(); delete b; return 0;
    } catch (...) { return invalid; } // On timeout leak listener/context, never free live callback targets.
}
int cn_yida_start(void* p) { try {
    auto* b=get(p); const char* configured=b->api->getConfig("TradingProtocol");
    const char* expected=b->protocol==0?"TCP":b->protocol==1?"UDP":"XTCP";
    if (!configured || std::strcmp(configured,expected) || b->instrument_allowlist.empty()) return invalid;
    return b->api->startExtended(b,b) ? 0 : invalid;
} catch(...) { return invalid; } }
int cn_yida_allow_instrument(void* p,const char* exchange,const char* symbol) { try {
    auto* b=get(p);
    if(!exchange || !symbol || b->tcp_connected || b->initialized) return local_invalid;
    if(std::strcmp(exchange,"SHFE") && std::strcmp(exchange,"INE")) return local_invalid;
    if(std::strlen(symbol)>=sizeof(YDInstrument::InstrumentID) || !*symbol) return local_invalid;
    b->instrument_allowlist.emplace_back(exchange,symbol); return 0;
} catch(...) { return invalid; } }
int cn_yida_login(void* p,const char* u,const char* pass,const char* app,const char* auth) { try { return get(p)->api->login(u,pass,app,auth) ? 0 : invalid; } catch(...) { return invalid; } }
int cn_yida_reference(void* p) { try { return get(p)->api->getNextOrderRef(get(p)->order_group,true); } catch(...) { return invalid; } }
int cn_yida_transport_ready(void* p) { try { return get(p)->can_send()?1:0; } catch(...) { return 0; } }
int cn_yida_public_simulation_target(void* p) { try {
    auto* api=get(p)->api;
    const char* host=api->getConfig("TradingServerIP");
    const char* port=api->getConfig("TradingServerPort");
    return host && port && !std::strcmp(host,"118.190.175.212") && !std::strcmp(port,"41600") ? 1 : 0;
} catch(...) { return 0; } }
int cn_yida_insert(void* p,const CnYidaOrderRequest* r) { try {
    if(!r) { return local_invalid; }
    auto* b=get(p); YDInputOrder o{}; const YDInstrument* i=nullptr;
    int code=b->make_order(*r,o,i); if(code) return code;
    return b->api->insertOrder(&o,i)?0:invalid;
} catch(...) { return invalid; } }
int cn_yida_cancel(void* p,const CnYidaCancelRequest* r) { try {
    if(!r) { return local_invalid; }
    auto* b=get(p); YDCancelOrder o{}; const YDExchange* x=nullptr;
    int code=b->make_cancel(*r,o,x); if(code) return code;
    return b->api->cancelOrder(&o,x)?0:invalid;
} catch(...) { return invalid; } }
int cn_yida_insert_batch(void* p,const CnYidaOrderRequest* r,int count,int* failed) { try {
    if(failed) *failed=0;
    if(!r || !failed || count<1 || count>16) return local_invalid;
    auto* b=get(p); YDInputOrder orders[16]{}; const YDInstrument* instruments[16]{};
    for(int k=0;k<count;++k) { *failed=k; int code=b->make_order(r[k],orders[k],instruments[k]); if(code) return code; }
    *failed=-1; return b->api->insertMultiOrders(count,orders,instruments)?0:invalid;
} catch(...) { return invalid; } }
int cn_yida_cancel_batch(void* p,const CnYidaCancelRequest* r,int count,int* failed) { try {
    if(failed) *failed=0;
    if(!r || !failed || count<1 || count>16) return local_invalid;
    auto* b=get(p); YDCancelOrder orders[16]{}; const YDExchange* exchanges[16]{};
    for(int k=0;k<count;++k) { *failed=k; int code=b->make_cancel(r[k],orders[k],exchanges[k]); if(code) return code; }
    *failed=-1; return b->api->cancelMultiOrders(count,orders,exchanges)?0:invalid;
} catch(...) { return invalid; } }
void cn_yida_disconnect(void* p) { try { get(p)->api->disconnect(); } catch(...) {} }
}
