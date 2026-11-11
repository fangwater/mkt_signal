#include "ctp_md_bridge.h"
#include "ThostFtdcMdApi.h"
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
template<size_t N, size_t M> void field(char (&out)[N], const char (&in)[M]) {
    auto size = strnlen(in, M); if(size >= N) size = N-1;
    std::memcpy(out,in,size); out[size]=0;
}
template<size_t N> void copy(char (&out)[N], const char* value) {
    if(!value || std::strlen(value)>=N) throw std::runtime_error("invalid MD request field");
    std::memcpy(out,value,std::strlen(value)+1);
}
void message(char* out,size_t size,const char* value) {if(out && size) std::snprintf(out,size,"%s",value);}
class Library {
    void* handle;
public:
    explicit Library(const char* path):handle(dlopen(path,RTLD_NOW|RTLD_LOCAL)) {if(!handle) throw std::runtime_error("cannot load MdApi library");}
    ~Library(){dlclose(handle);}
    template<typename T> T symbol(const char* name) {
        dlerror();auto result=dlsym(handle,name);if(dlerror()) throw std::runtime_error("missing MdApi symbol");return reinterpret_cast<T>(result);
    }
    const char* version(){return symbol<decltype(&CThostFtdcMdApi::GetApiVersion)>("_ZN15CThostFtdcMdApi13GetApiVersionEv")();}
};
class Spi final:public CThostFtdcMdSpi {
    NfMdCallback callback;void* context;
    void send(NfMdEvent& e,CThostFtdcRspInfoField* info=nullptr,int id=0,bool last=true) {
        e.request=id;e.last=last;if(info){e.error=info->ErrorID;field(e.message,info->ErrorMsg);} callback(context,&e);
    }
public:
    Spi(NfMdCallback cb,void* ctx):callback(cb),context(ctx){}
    void OnFrontConnected() override {NfMdEvent e{};e.kind=1;send(e);}
    void OnFrontDisconnected(int reason) override {NfMdEvent e{};e.kind=2;e.error=reason;send(e);}
    void OnRspUserLogin(CThostFtdcRspUserLoginField* p,CThostFtdcRspInfoField* i,int r,bool l) override {
        NfMdEvent e{};e.kind=4;if(p){e.present=1;field(e.trading_day,p->TradingDay);}send(e,i,r,l);
    }
    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField* p,CThostFtdcRspInfoField* i,int r,bool l) override {
        NfMdEvent e{};e.kind=21;if(p){e.present=1;field(e.symbol,p->InstrumentID);}send(e,i,r,l);
    }
    void OnRspError(CThostFtdcRspInfoField* i,int r,bool l) override {NfMdEvent e{};e.kind=16;send(e,i,r,l);}
    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* p) override {
        if(!p) return;
        NfMdEvent e{};e.kind=22;e.present=1;
        field(e.symbol,p->InstrumentID);field(e.exchange,p->ExchangeID);field(e.trading_day,p->TradingDay);
        field(e.action_day,p->ActionDay);field(e.update_time,p->UpdateTime);e.millisec=p->UpdateMillisec;
        e.volume=p->Volume;e.last_price=p->LastPrice;e.turnover=p->Turnover;e.open_interest=p->OpenInterest;
        e.upper_limit=p->UpperLimitPrice;e.lower_limit=p->LowerLimitPrice;
#define LEVEL(n) e.bid[n-1]=p->BidPrice##n;e.ask[n-1]=p->AskPrice##n;e.bid_volume[n-1]=p->BidVolume##n;e.ask_volume[n-1]=p->AskVolume##n;
        LEVEL(1) LEVEL(2) LEVEL(3) LEVEL(4) LEVEL(5)
#undef LEVEL
        send(e);
    }
};
struct Handle {
    Library library;Spi spi;CThostFtdcMdApi* api=nullptr;std::vector<std::string> fronts;
    Handle(const char* path,const char* flow,bool production,NfMdCallback cb,void* ctx):library(path),spi(cb,ctx) {
        const char* version=library.version();const char* numeric=version && version[0]=='v'?version+1:version;
        if(!numeric || std::strncmp(numeric,"6.7.11",6) || (numeric[6] && numeric[6]!='_' && numeric[6]!='.')) throw std::runtime_error("official CTP 6.7.11 MD SDK required");
        api=library.symbol<decltype(&CThostFtdcMdApi::CreateFtdcMdApi)>("_ZN15CThostFtdcMdApi15CreateFtdcMdApiEPKcbbb")(flow,false,false,production);
        if(!api) throw std::runtime_error("MdApi factory returned null");
        api->RegisterSpi(&spi);
    }
    ~Handle(){if(api){api->RegisterSpi(nullptr);api->Release();}}
};
template<typename F> int request(void* h,F f){try {if(!h)return -9901;return f(*static_cast<Handle*>(h));}catch(...){return -9901;}}
}
extern "C" size_t nf_md_event_size(){return sizeof(NfMdEvent);}
extern "C" int nf_md_version(const char* path,char* out,size_t size){try{Library library(path);message(out,size,library.version());return 0;}catch(const std::exception& e){message(out,size,e.what());return -9901;}}
extern "C" void* nf_md_create(const char* path,const char* flow,bool production,NfMdCallback cb,void* ctx,char* error,size_t size){try{return new Handle(path,flow,production,cb,ctx);}catch(const std::exception& e){message(error,size,e.what());return nullptr;}}
extern "C" void nf_md_destroy(void* h){delete static_cast<Handle*>(h);}
extern "C" int nf_md_start(void* h,const char* fronts){return request(h,[&](Handle& x){
    std::string input(fronts);size_t offset=0;
    for(;;){auto end=input.find(',',offset);x.fronts.push_back(input.substr(offset,end==std::string::npos?end:end-offset));if(end==std::string::npos)break;offset=end+1;}
    for(auto& front:x.fronts)x.api->RegisterFront(front.data());
    x.api->Init();return 0;
});}
extern "C" int nf_md_login(void* h,int id,const char* broker,const char* user,const char* password){return request(h,[&](Handle& x){CThostFtdcReqUserLoginField r{};copy(r.BrokerID,broker);copy(r.UserID,user);copy(r.Password,password);return x.api->ReqUserLogin(&r,id);});}
extern "C" int nf_md_subscribe(void* h,const char* const* symbols,size_t count){return request(h,[&](Handle& x){if(!count || count>1000)return -9901;std::vector<char*> ids;for(size_t i=0;i<count;++i)ids.push_back(const_cast<char*>(symbols[i]));return x.api->SubscribeMarketData(ids.data(),static_cast<int>(count));});}
