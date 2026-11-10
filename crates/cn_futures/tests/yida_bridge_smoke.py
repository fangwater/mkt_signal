#!/usr/bin/env python3
"""Compile a synthetic native YDApi against the reference headers and exercise Rust/C++.

No vendor library is loaded and no network endpoints are contacted. Unused virtual
methods are generated from the real headers, not a separately maintained SDK ABI.
"""
import argparse
import json
import os
from pathlib import Path
import queue
import re
import shutil
import subprocess
import tempfile
import threading
import time

MOCK = r'''
#include "ydApi.h"
#include <cstring>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>
#include <fstream>
#include <cstdlib>
static void check(bool v) { if (!v) throw std::runtime_error("fixture validation failed"); }
template<class T> class Rows final:public YDQueryResult<T> {
    std::vector<const T*> rows;
public:
    explicit Rows(std::vector<const T*> r):rows(std::move(r)){}
    int getCount()const override{return rows.size();}
    const T* get(int i)const override{return rows.at(i);}
    void destroy()override{delete this;}
};
class Fixture final:public YDExtendedApi {
    YDListener* listener=nullptr; YDExtendedListener* extended=nullptr;
    YDAccount account{}; YDExtendedAccount cash{};
    YDExchange exchanges[2]{}; YDProduct products[2]{}; YDInstrument instruments[2]{};
    YDMarketData market_data[2]{};
    YDAccountExchangeInfo ax[2]{}; YDAccountProductInfo ap[2]{}; YDAccountInstrumentInfo ai[2]{};
    YDExtendedPosition positions[8]{};
    std::map<std::pair<int,long long>,YDExtendedOrder> orders;
    std::vector<YDExtendedTrade> trades;
    int next_reference=101; bool initialized=false, xtcp_initialized=false;
    std::string protocol;
    std::map<std::string,std::string> config_values;
    YDInstrument option{}; YDAccountInstrumentInfo option_ai{}; YDExtendedPosition option_position{};
    YDSystemParam port{};
    static void copy_input(YDOrder& o,const YDInputOrder& p){
        o.Direction=p.Direction;o.OffsetFlag=p.OffsetFlag;o.HedgeFlag=p.HedgeFlag;
        o.Price=p.Price;o.OrderVolume=p.OrderVolume;o.OrderRef=p.OrderRef;
        o.OrderType=p.OrderType;o.YDOrderFlag=p.YDOrderFlag;o.OrderGroupID=p.OrderGroupID;
        o.ExchangeOrderAttribute=p.ExchangeOrderAttribute;
    }
    int index(const YDInstrument* p){return p==&instruments[0]?0:1;}
    void fill(YDExtendedOrder& o,int lots,int terminal_first=-1){
        const int k=index(o.m_pInstrument);YDExtendedTrade t{};
        t.m_pInstrument=o.m_pInstrument;t.m_pAccount=&account;
        t.Direction=o.Direction;t.OffsetFlag=o.OffsetFlag;t.HedgeFlag=o.HedgeFlag;
        t.OrderRef=o.OrderRef;t.OrderSysID=o.OrderSysID;t.LongOrderSysID=o.LongOrderSysID;
        t.TradeID=55;t.LongTradeID=(1LL<<40)+1000+trades.size();t.Price=o.ExchangeOrderAttribute?4000:o.Price;t.Volume=lots;t.Commission=1.25;
        const int side=o.OffsetFlag==YD_OF_Open?o.Direction:1-o.Direction;
        const int date=o.OffsetFlag==YD_OF_CloseYesterday?1:0;
        auto& pos=positions[k*4+side*2+date];
        if(o.OffsetFlag==YD_OF_Open)pos.Position+=lots;else {pos.Position-=lots;pos.CloseFrozen-=lots;}
        o.TradeVolume+=lots;trades.push_back(t);
        if(terminal_first>=0){o.OrderStatus=terminal_first;listener->notifyOrder(&o,o.m_pInstrument,&account);}
        listener->notifyTrade(&t,o.m_pInstrument,&account);listener->notifyTrade(&t,o.m_pInstrument,&account);
    }
public:
    explicit Fixture(const char* config){
        std::ifstream input(config);std::string line;
        while(std::getline(input,line)){auto eq=line.find('=');if(eq!=std::string::npos)config_values[line.substr(0,eq)]=line.substr(eq+1);}
        protocol=config_values.at("TradingProtocol");
        const char* test_port=std::getenv("YIDA_FIXTURE_PORT");std::strcpy(port.Value,test_port?test_port:"41601");
        std::strcpy(account.AccountID,"fixture");account.LoginCount=1;account.TradingRight=YD_TR_Allow;
        if(std::getenv("YIDA_FIXTURE_MULTI_LOGIN"))account.LoginCount=2;
        if(std::getenv("YIDA_FIXTURE_CLOSE_ONLY"))account.TradingRight=YD_TR_CloseOnly;
        cash.m_pAccount=&account;cash.Available=1000000;
        for(int k=0;k<2;++k){
            std::strcpy(exchanges[k].ExchangeID,k?"INE":"SHFE");exchanges[k].UseTodayPosition=true;exchanges[k].ExchangeRef=k+1;
            std::strcpy(products[k].ProductID,k?"sc":"cu");products[k].m_pExchange=&exchanges[k];
            std::strcpy(instruments[k].InstrumentID,k?"sc2612":"cu2612");instruments[k].m_pExchange=&exchanges[k];instruments[k].m_pProduct=&products[k];
            instruments[k].ProductClass=YD_PC_Futures;instruments[k].Tick=.2;instruments[k].Multiple=5;instruments[k].ExpireDate=20261220;
            instruments[k].MinLimitOrderVolume=1;instruments[k].MaxLimitOrderVolume=100;
            instruments[k].MinMarketOrderVolume=1;instruments[k].MaxMarketOrderVolume=10;
            instruments[k].m_pMarketData=&market_data[k];market_data[k].UpperLimitPrice=6000;market_data[k].LowerLimitPrice=3000;
            if(std::getenv("YIDA_FIXTURE_NO_MARKET_LIMITS"))instruments[k].m_pMarketData=nullptr;
            ax[k].m_pAccount=&account;ax[k].m_pExchange=&exchanges[k];ap[k].m_pAccount=&account;ap[k].m_pProduct=&products[k];ai[k].m_pAccount=&account;ai[k].m_pInstrument=&instruments[k];
            for(int side=0;side<2;++side)for(int date=0;date<2;++date){
                auto& p=positions[k*4+side*2+date];p.m_pAccountInstrumentInfo=&ai[k];
                p.PositionDate=date?YD_PSD_History:YD_PSD_Today;p.PositionDirection=side?YD_PD_Short:YD_PD_Long;
                p.HedgeFlag=YD_HF_Speculation;p.Position=(!k && !side)?(date?3:2):0;
            }
            // Same system ID and manual reference zero on different exchanges.
            YDExtendedOrder o{};o.m_pAccount=&account;o.m_pInstrument=&instruments[k];o.OrderSysID=55;o.LongOrderSysID=(1LL<<40)+55;o.OrderRef=0;
            o.HedgeFlag=YD_HF_Speculation;o.OrderVolume=1;o.Price=.2;o.OrderStatus=YD_OS_Queuing;
            if(std::getenv("YIDA_FIXTURE_OPTION_POSITION"))o.OrderStatus=YD_OS_Canceled;
            orders[{k,o.LongOrderSysID}]=o;
        }
        if(std::getenv("YIDA_FIXTURE_ONE_EXISTING_LOT")){
            for(auto& p:positions)p.Position=0;positions[0].Position=1;
        }
        std::strcpy(option.InstrumentID,"sc2701C790");option.ProductClass=YD_PC_Options;option.m_pExchange=&exchanges[1];
        option_ai.m_pAccount=&account;option_ai.m_pInstrument=&option;option_position.m_pAccountInstrumentInfo=&option_ai;
        option_position.Position=19;option_position.PositionDate=YD_PSD_History;option_position.PositionDirection=YD_PD_Long;option_position.HedgeFlag=YD_HF_Speculation;
    }
    bool start(YDListener* l)override{listener=l;listener->notifyEvent(YD_AE_TCPTradeConnected);listener->notifyReadyForLogin(false);return true;}
    bool startExtended(YDListener* l,YDExtendedListener* e)override{extended=e;return start(l);}
    void startDestroy()override{auto* l=listener;if(l){std::thread([this,l]{std::this_thread::sleep_for(std::chrono::milliseconds(5));l->notifyBeforeApiDestroy();delete this;l->notifyAfterApiDestroy();}).detach();}else{delete this;}}
    bool login(const char* u,const char* p,const char* a,const char* auth)override{
        check(std::strcmp(u,"fixture")==0 && std::strcmp(p,"fixture-only")==0 && std::strcmp(a,"fixture-app")==0 && std::strcmp(auth,"fixture-auth")==0);
        listener->notifyLogin(0,next_reference-1,false);
        if(protocol=="XTCP" && !std::getenv("YIDA_FIXTURE_NO_XTCP") && !std::getenv("YIDA_FIXTURE_LATE_XTCP")
            && (!std::getenv("YIDA_FIXTURE_RETAIN_XTCP") || !xtcp_initialized)){
            xtcp_initialized=true;listener->notifyEvent(YD_AE_XTCPTradeConnected);
        }
        if(!initialized){initialized=true;listener->notifyFinishInit();}
        if(std::getenv("YIDA_FIXTURE_ACCOUNT_HISTORY")){
            int current=account.LoginCount;account.LoginCount=0;extended->notifyExtendedAccount(&cash);account.LoginCount=current;
        }
        if(std::getenv("YIDA_FIXTURE_REJECT_HISTORY")){
            YDOrder o{};o.OrderRef=77;o.HedgeFlag=YD_HF_Speculation;o.OrderVolume=1;
            o.OrderStatus=YD_OS_Rejected;o.ErrorNo=40;o.ExchangeOrderAttribute=YD_EOA_SHFE_MarketPrice;
            listener->notifyOrder(&o,&instruments[0],&account);
        }
        listener->notifyCaughtUp();
        if(protocol=="XTCP" && std::getenv("YIDA_FIXTURE_LATE_XTCP"))listener->notifyEvent(YD_AE_XTCPTradeConnected);
        return true;
    }
    void disconnect()override{listener->notifyEvent(YD_AE_TCPTradeDisconnected);listener->notifyEvent(YD_AE_TCPTradeConnected);listener->notifyReadyForLogin(false);}
    int getTradingDay()override{return 20261109;}
    int getNextOrderRef(unsigned group=0,bool update=true)override{check(group==0 && update);return next_reference++;}
    const char* getConfig(const char* name)override{auto it=config_values.find(name);return it==config_values.end()?nullptr:it->second.c_str();}
    const YDSystemParam* getSystemParamByName(const char* name,const char* target)override{
        check(!std::strcmp(target,"System") && (!std::strcmp(name,"UDPTradingPort") || !std::strcmp(name,"XTCPTradingPort")));return &port;
    }
    const YDAccount* getMyAccount()override{return &account;}
    const YDExtendedAccount* getExtendedAccount(const YDAccount* =nullptr)override{return &cash;}
    int getInstrumentCount()override{return 2;}
    const YDInstrument* getInstrument(int n)override{return &instruments[n];}
    const YDInstrument* getInstrumentByID(const char* s)override{for(auto& i:instruments)if(!std::strcmp(i.InstrumentID,s))return &i;return nullptr;}
    const YDExchange* getExchangeByID(const char* s)override{for(auto& x:exchanges)if(!std::strcmp(x.ExchangeID,s))return &x;return nullptr;}
    const YDAccountExchangeInfo* getAccountExchangeInfo(const YDExchange* x,const YDAccount* a=nullptr)override{check(!a);return &ax[x==&exchanges[0]?0:1];}
    const YDAccountProductInfo* getAccountProductInfo(const YDProduct* p,const YDAccount* a=nullptr)override{check(!a);return &ap[p==&products[0]?0:1];}
    const YDAccountInstrumentInfo* getAccountInstrumentInfo(const YDInstrument* i,const YDAccount* a=nullptr)override{check(!a);return &ai[index(i)];}
    const char* getIDFromExchange(const YDExchange*,int type,int)override{return type==YD_IDT_TradeID?"exchange-trade-id":"exchange-order-id";}
    YDQueryResult<YDExtendedPosition>* findExtendedPositions(const YDExtendedPositionFilter* f)override{check(f->HedgeFlag==-1 && f->PositionDate==-1 && !f->pAccount);std::vector<const YDExtendedPosition*> r;for(auto& p:positions)r.push_back(&p);if(std::getenv("YIDA_FIXTURE_OPTION_POSITION"))r.push_back(&option_position);return new Rows<YDExtendedPosition>(r);}
    unsigned findExtendedPositions(const YDExtendedPositionFilter*,unsigned,const YDExtendedPosition*[])override{return 0;}
    YDQueryResult<YDExtendedOrder>* findOrders(const YDOrderFilter* f)override{check(f->StartTime==-1 && f->EndTime==-1 && f->YDOrderFlags==-1 && !f->pAccount);std::vector<const YDExtendedOrder*> r;for(auto& pair:orders)r.push_back(&pair.second);return new Rows<YDExtendedOrder>(r);}
    unsigned findOrders(const YDOrderFilter*,unsigned,const YDExtendedOrder*[])override{return 0;}
    YDQueryResult<YDExtendedTrade>* findTrades(const YDTradeFilter* f)override{check(f->StartTime==-1 && f->EndTime==-1 && !f->pAccount);std::vector<const YDExtendedTrade*> r;for(auto& t:trades)r.push_back(&t);return new Rows<YDExtendedTrade>(r);}
    unsigned findTrades(const YDTradeFilter*,unsigned,const YDExtendedTrade*[])override{return 0;}
    YDQueryResult<YDExtendedTrade>* findTrades(const YDTradeFilter2* f)override{return findTrades(static_cast<const YDTradeFilter*>(f));}
    bool checkOrder(YDInputOrder* o,const YDInstrument*,const YDAccount* a=nullptr)override{
        check(!a && o->OrderRef>100 && o->HedgeFlag==YD_HF_Speculation && o->YDOrderFlag==YD_YOF_Normal && o->ConnectionSelectionType==YD_CS_Any);
        check(o->OrderType==YD_ODT_Limit || o->OrderType==YD_ODT_FAK || o->OrderType==YD_ODT_FOK);
        check(o->ExchangeOrderAttribute==0 || o->ExchangeOrderAttribute==YD_EOA_SHFE_MarketPrice);
        check(o->ExchangeOrderAttribute ? o->Price==(o->Direction==YD_D_Buy?6000:3000) : o->Price>0);
        check(o->OffsetFlag==YD_OF_Open || o->OffsetFlag==YD_OF_CloseToday || o->OffsetFlag==YD_OF_CloseYesterday);
        if(o->Price==5000){o->ErrorNo=40;return false;}return true;
    }
    bool insertOrder(YDInputOrder* p,const YDInstrument* i,const YDAccount* a=nullptr)override{
        check(!a);if(p->Price==5001){p->ErrorNo=80;return false;}
        if(p->Price==5002){YDOrder e{};copy_input(e,*p);e.ErrorNo=25;e.OrderStatus=YD_OS_Rejected;listener->notifyOrder(&e,i,&account);return true;}
        if(p->Price==5004){listener->notifyEvent(YD_AE_ServerRestarted);return true;}
        if(p->Price==5006)return true; // Simulated lost UDP packet: successful local send, no acknowledgement.
        YDOrder pending{};copy_input(pending,*p);pending.OrderStatus=YD_OS_Accepted;
        listener->notifyOrder(&pending,i,&account); // Server accepted; exchange system ID still unavailable.
        YDExtendedOrder o{};copy_input(o,*p);o.OrderSysID=55;o.LongOrderSysID=(1LL<<40)+p->OrderRef+1000;
        o.m_pInstrument=i;o.m_pAccount=&account;o.OrderStatus=YD_OS_Queuing;
        auto key=std::make_pair(index(i),o.LongOrderSysID);orders[key]=o;auto& stored=orders.at(key);
        if(p->OffsetFlag!=YD_OF_Open)positions[index(i)*4+(1-p->Direction)*2+(p->OffsetFlag==YD_OF_CloseYesterday?1:0)].CloseFrozen+=p->OrderVolume;
        if(p->Price==5008){fill(stored,p->OrderVolume,YD_OS_AllTraded);return true;}
        if(p->Price==5009){fill(stored,1,YD_OS_Canceled);listener->notifyOrder(&stored,i,&account);return true;}
        if(p->Price==5010){fill(stored,1);auto conflict=trades.back();conflict.Price+=.2;listener->notifyTrade(&conflict,i,&account);return true;}
        if(p->Price==5011){stored.OrderStatus=99;listener->notifyOrder(&stored,i,&account);return true;}
        if(p->OrderType!=YD_ODT_Limit || p->ExchangeOrderAttribute){fill(stored,p->OrderVolume);stored.OrderStatus=YD_OS_AllTraded;}
        listener->notifyOrder(&stored,i,&account);
        if(p->Price==5005 && protocol=="XTCP"){xtcp_initialized=false;listener->notifyEvent(YD_AE_XTCPTradeDisconnected);}
        return true;
    }
    bool cancelOrder(YDCancelOrder* p,const YDExchange* x,const YDAccount* a=nullptr)override{
        check(!a && p->LongOrderSysID>(1LL<<32) && p->OrderSysID==0 && p->ConnectionSelectionType==YD_CS_Any && p->YDOrderFlag==YD_YOF_Normal);
        auto& o=orders.at({x==&exchanges[0]?0:1,p->LongOrderSysID});
        if(o.Price==5003){YDFailedCancelOrder e{};e.OrderSysID=o.OrderSysID;e.LongOrderSysID=o.LongOrderSysID;e.ErrorNo=31;listener->notifyFailedCancelOrder(&e,x,&account);
            e.LongOrderSysID=0;e.OrderGroupID=3;listener->notifyFailedCancelOrder(&e,x,&account);return true;}
        if(o.Price==5007)return true;
        if(o.OrderVolume-o.TradeVolume>0)fill(o,1);
        if(o.OffsetFlag!=YD_OF_Open)positions[index(o.m_pInstrument)*4+(1-o.Direction)*2+(o.OffsetFlag==YD_OF_CloseYesterday?1:0)].CloseFrozen-=o.OrderVolume-o.TradeVolume;
        o.OrderStatus=YD_OS_Canceled;listener->notifyOrder(&o,o.m_pInstrument,&account);return true;
    }
    // GENERATED_STUBS
};
extern "C" YDExtendedApi* makeYDExtendedApi(const char* config){return new Fixture(config);}
extern "C" const char* getYDVersion(){return "1.502.53.182";}
'''

def fixture_source(sdk):
    header=(sdk/'ydApi.h').read_text()
    implemented=set(re.findall(r'\b(\w+)\([^;{}]*\)\s*override\s*\{',MOCK))
    stubs=[]
    for signature in re.findall(r'virtual\s+([^;{}]+?)\s*=\s*0\s*;',header):
        signature=' '.join(signature.split())
        name=re.search(r'(\w+)\s*\(',signature).group(1)
        if name not in implemented:
            body='' if signature.startswith('void ') else 'return {};'
            stubs.append(signature+' override { '+body+' }')
    return MOCK.replace('// GENERATED_STUBS','\n'.join(stubs))

def run_fixture(args,protocol,scenario='normal'):
    root=Path(__file__).resolve().parents[3]
    with tempfile.TemporaryDirectory(prefix='mkt-yida-fixture-') as directory:
        temp=Path(directory);source=temp/'fixture.cpp';library=temp/'fixture.so'
        source.write_text(fixture_source(args.sdk_dir))
        subprocess.run(['c++','-std=c++17','-shared','-fPIC','-pthread','-isystem',str(args.sdk_dir),str(source),'-o',str(library)],check=True)
        if scenario=='public_initial_position':
            fake_sdk=temp/'sdk';(fake_sdk/'linux64').mkdir(parents=True)
            os.link(library,fake_sdk/'linux64/yd.so')
            # Test-only alias: the SDK remains synthetic and cannot contact a venue.
            test_binary=temp/'release/yida-trader';test_binary.parent.mkdir()
            shutil.copy2(args.binary.resolve(),test_binary)
            environment={k:v for k,v in os.environ.items() if not k.startswith('YIDA_FIXTURE_')}
            environment.update(YIDA_USER_ID='fixture',YIDA_PASSWORD='fixture-only',YIDA_APP_ID='fixture-app',YIDA_AUTH_CODE='fixture-auth',YIDA_FIXTURE_OPTION_POSITION='1',YIDA_FIXTURE_ONE_EXISTING_LOT='1')
            audit=temp/'audit'
            result=subprocess.run(['python3',str(root/'crates/cn_futures/tests/yida_public_sim_smoke.py'),'--sdk-root',str(fake_sdk),'--binary',str(test_binary),'--output-dir',str(audit),'--protocol','xtcp','--venue','SHFE','--symbol','cu2612','--passive-price-ticks','20000','--buy-price-ticks','20001','--sell-price-ticks','19999','--execute'],env=environment,capture_output=True,text=True,timeout=25)
            assert result.returncode==1,(result.stdout,result.stderr)
            summary=json.loads((audit/'summary.json').read_text())
            assert 'initially be flat' in summary['error'] and summary['cleanup_error'] is None
            events=[json.loads(line) for line in (audit/'events.jsonl').read_text().splitlines()]
            assert not any(e.get('type') in ('prepared_submit','prepared_cancel') for e in events)
            print(json.dumps({'protocol':protocol,'scenario':scenario,'preexisting_position_untouched':True,'no_send':True}));return
        native_config=temp/'server.cfg'
        native_text=f'TradingServerIP=127.0.0.1\nTradingServerPort=1\nTradingProtocol={protocol.upper()}\nUDPTradingType=socket\nXTCPTradingType=socket\nConnectTCPMarketData=no\nReceiveUDPMarketData=no\nRecalcMode=off\n'
        config=(root/'config/yida_trade.example.toml').read_text()
        for old,new in [('/opt/yida/1.502.53.182/linux64/yd.so',str(library)),('/opt/yida/trade-server.cfg',str(native_config)),('trading_protocol = "xtcp"',f'trading_protocol = "{protocol}"'),('/tmp/mkt_yida_sim/flow',str(temp/'flow')),('/tmp/mkt_yida_sim/events.jsonl',str(temp/'events.jsonl')),('cash_budget_per_open_lot = 250000.0','cash_budget_per_open_lot = 10000.0'),('max_order_lots = 1','max_order_lots = 10'),('max_position_lots_per_side = 2','max_position_lots_per_side = 20'),('max_working_lots = 2','max_working_lots = 20'),('max_actions_per_second = 2','max_actions_per_second = 100'),('enable_fak_fok = false','enable_fak_fok = true'),('response_timeout_ms = 30000','response_timeout_ms = 5000')]:config=config.replace(old,new)
        if scenario=='affinity':
            allowed_cpus=sorted(os.sched_getaffinity(0));business_core,sdk_core=allowed_cpus[:2]
            config=config.replace('# business_core = <isolated CPU>;',f'business_core = {business_core} #')
            native_text+=f'TCPTradingCPUID={sdk_core}\nTimerCPUID={sdk_core}\nXTCPTradingCPUID={sdk_core}\n'
        if scenario=='public_simulation':
            native_text=native_text.replace('127.0.0.1','118.190.175.212').replace('TradingServerPort=1\n','TradingServerPort=41600\n')
            config=config.replace('public_simulation = false','public_simulation = true').replace('max_order_lots = 10','max_order_lots = 1').replace('max_working_lots = 20','max_working_lots = 2')
        native_config.write_text(native_text)
        config_path=temp/'config.toml';config_path.write_text(config)
        environment={k:v for k,v in os.environ.items() if not k.startswith('YIDA_FIXTURE_')}
        environment.update(YIDA_USER_ID='fixture',YIDA_PASSWORD='fixture-only',YIDA_APP_ID='fixture-app',YIDA_AUTH_CODE='fixture-auth')
        if scenario=='market_no_limits':environment['YIDA_FIXTURE_NO_MARKET_LIMITS']='1'
        if scenario=='no_port':environment['YIDA_FIXTURE_PORT']='0'
        if scenario=='no_xtcp':environment['YIDA_FIXTURE_NO_XTCP']='1'
        if scenario=='multi_login':environment['YIDA_FIXTURE_MULTI_LOGIN']='1'
        if scenario=='close_only':environment['YIDA_FIXTURE_CLOSE_ONLY']='1'
        if scenario=='public_simulation':environment['YIDA_FIXTURE_OPTION_POSITION']='1'
        if scenario=='late_xtcp':environment['YIDA_FIXTURE_LATE_XTCP']='1'
        if scenario=='account_history':environment['YIDA_FIXTURE_ACCOUNT_HISTORY']='1'
        if scenario=='reject_history':environment['YIDA_FIXTURE_REJECT_HISTORY']='1'
        if scenario=='retained_xtcp':environment['YIDA_FIXTURE_RETAIN_XTCP']='1'
        with open(temp/'stderr.log','w+') as stderr:
            argv=[str(args.binary.resolve()),'--config',str(config_path),'--connect']
            if scenario!='read_only':argv.append('--execute')
            process=subprocess.Popen(argv,env=environment,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True)
            responses=queue.Queue()
            threading.Thread(target=lambda:[responses.put(json.loads(line)) for line in process.stdout],daemon=True).start()
            def send(value):process.stdin.write(json.dumps(value)+'\n');process.stdin.flush()
            def wait(kind,timeout=10):
                deadline=time.monotonic()+timeout
                while time.monotonic()<deadline:
                    try:value=responses.get(timeout=.2)
                    except queue.Empty:
                        if process.poll() is not None:
                            stderr.seek(0);raise AssertionError(stderr.read())
                        continue
                    if value.get('type')==kind:return value
                    if value.get('type') in ('trading_blocked','command_rejected'):raise AssertionError(value)
                raise AssertionError('timed out waiting for '+kind)
            def full_state():send({'op':'state'});return wait('state')
            def state():return full_state()['state']
            def submit(offset='open',lots=1,tif='day',ticks=20000,price_type='limit',venue='SHFE',symbol='cu2612'):
                send({'op':'submit','intent':{'venue':venue,'symbol':symbol,'side':'sell','offset':offset,'lots':lots,'price_type':price_type,'price_ticks':ticks,'time_in_force':tif}})
                return wait('submit_receipt')['receipt']
            try:
                if scenario=='multi_login':
                    assert 'exactly one login' in wait('trading_blocked')['error']
                    assert not full_state()['can_trade']
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'account_guard':True}));return
                initial=wait('reconciled')['state'];assert initial['ready']
                assert len([o for o in initial['orders'] if o['key']['order_ref']=='0'])==2
                assert all(int(o['system_id'])>(1<<32) for o in initial['orders'])
                if scenario=='public_simulation':
                    before=full_state();assert before['can_trade'] and len(before['read_only_positions'])==1
                    key=submit()['children'][0]['key'];send({'op':'cancel','key':key});wait('cancel_submitted_locally')
                    after=full_state();assert len(after['read_only_positions'])==1 and after['read_only_positions'][0]['lots']==19
                    send({'op':'reconcile'});wait('reconciling');wait('reconciled');assert full_state()['can_trade']
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'futures_submit_cancel':True,'option_inventory_retained':True}));return
                if scenario=='read_only':
                    assert not full_state()['can_trade']
                    send({'op':'submit','intent':{'venue':'SHFE','symbol':'cu2612','side':'sell','offset':'open','lots':1,'price_ticks':20000,'time_in_force':'day'}})
                    assert 'read-only' in wait('command_rejected')['error']
                    send({'op':'cancel','key':initial['orders'][0]['key']});assert 'read-only' in wait('command_rejected')['error']
                    assert len(state()['orders'])==2
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'no_send':True}));return
                if scenario=='close_only':
                    assert submit()['children'][0]['outcome']=='rejected_locally'
                    assert submit('close_today',1,'fak')['all_submitted_locally']
                    assert next(p for p in state()['positions'] if p['symbol']=='cu2612' and p['side']=='buy')['position']['today']==1
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'open_rejected_close_allowed':True}));return
                if scenario=='affinity':
                    assert full_state()['can_trade']
                    assert os.sched_getaffinity(process.pid)=={business_core}
                    workers=[task for task in Path(f'/proc/{process.pid}/task').iterdir() if (task/'comm').read_text().strip().startswith('cn-futures-')]
                    assert workers
                    assert all(os.sched_getaffinity(int(task.name))==set(allowed_cpus) for task in workers)
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'business_thread_pinned':True,'background_threads_keep_original_affinity':True}));return
                if scenario in ('no_port','no_xtcp'):
                    before=full_state();assert not before['transport_ready'] and not before['can_trade']
                    send({'op':'submit','intent':{'venue':'SHFE','symbol':'cu2612','side':'sell','offset':'open','lots':1,'price_ticks':20000,'time_in_force':'day'}})
                    assert 'transport unavailable' in wait('command_rejected')['error']
                    assert len(state()['orders'])==2
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'no_send':True}));return
                if scenario=='ack_timeout':
                    receipt=submit(ticks=25030);key=receipt['children'][0]['key'];assert receipt['all_submitted_locally']
                    blocked=wait('trading_blocked',timeout=8);assert blocked['request']=={'operation':'submit','order_ref':int(key['order_ref'])}
                    after=full_state();assert not after['can_trade']
                    order=next(o for o in after['state']['orders'] if o['key']==key)
                    assert order['status']=='pending' and order['local_cash_held']>0
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    events=[json.loads(l) for l in (temp/'events.jsonl').read_text().splitlines()]
                    assert sum(e.get('type')=='prepared_submit' for e in events)==1
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'reservation_retained':True,'automatic_retry':False}));return
                if scenario in ('cancel_timeout','cancel_error_timeout'):
                    receipt=submit(lots=2,ticks=25015 if scenario=='cancel_error_timeout' else 25035);key=receipt['children'][0]['key']
                    before=next(o for o in state()['orders'] if o['key']==key)
                    send({'op':'cancel','key':key});wait('cancel_submitted_locally')
                    pending=next(o for o in state()['orders'] if o['key']==key);assert pending['cancel_pending']
                    blocked=wait('trading_blocked',timeout=8);assert blocked['request']['operation']=='cancel'
                    after=full_state();assert not after['can_trade']
                    order=next(o for o in after['state']['orders'] if o['key']==key)
                    assert order['local_cash_held']==before['local_cash_held'] and order['cancel_pending']
                    send({'op':'reconcile'});wait('reconciling');wait('reconciled');assert full_state()['can_trade']
                    assert not next(o for o in state()['orders'] if o['key']==key)['cancel_pending']
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'reservation_retained':True,'manual_reconcile':True}));return
                if scenario in ('terminal_first','partial_cancel_first','conflicting_fill','malformed_report'):
                    ticks={'terminal_first':25040,'partial_cancel_first':25045,'conflicting_fill':25050,'malformed_report':25055}[scenario]
                    receipt=submit(lots=3,ticks=ticks);key=receipt['children'][0]['key'];assert receipt['all_submitted_locally']
                    if scenario in ('conflicting_fill','malformed_report'):wait('trading_blocked')
                    after=full_state();order=next(o for o in after['state']['orders'] if o['key']==key)
                    if scenario=='terminal_first':assert order['status']=='filled' and order['filled']==3
                    if scenario=='partial_cancel_first':assert order['status']=='cancelled' and order['filled']==1 and order['cancelled']==2
                    if scenario=='conflicting_fill':assert not after['can_trade'] and order['filled']==1
                    if scenario=='malformed_report':assert not after['can_trade'] and order['filled']==0
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'callback_ordering_and_validation':True}));return
                assert full_state()['can_trade']
                if scenario in ('market','market_no_limits'):
                    for item in state()['orders']:
                        if item['status']=='working':
                            send({'op':'cancel','key':item['key']});wait('cancel_submitted_locally')
                    if scenario=='market_no_limits':
                        before=state()['available_local_budget']
                        receipt=submit(ticks=0,price_type='market')
                        assert receipt['children'][0]['outcome']=='rejected_locally' and '9903' in receipt['children'][0]['error']
                        assert full_state()['can_trade'] and state()['available_local_budget']==before
                        send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                        print(json.dumps({'protocol':protocol,'scenario':scenario,'native_send':False,'reservation_released':True}));return
                    keys=[]
                    for venue,symbol in [('SHFE','cu2612'),('INE','sc2612')]:
                        for tif in ('day','fak','fok'):
                            receipt=submit(tif=tif,ticks=0,price_type='market',venue=venue,symbol=symbol)
                            assert receipt['all_submitted_locally'];key=receipt['children'][0]['key'];keys.append(key)
                            item=next(o for o in state()['orders'] if o['key']==key)
                            assert item['status']=='filled' and item['filled']==1 and item['request']['price_type']=='market'
                    send({'op':'reconcile'});wait('reconciling');restored=wait('reconciled')['state']
                    assert restored['ready']
                    assert all(next(o for o in restored['orders'] if o['key']==key)['request']['price_type']=='market' for key in keys)
                    send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                    events=[json.loads(l) for l in (temp/'events.jsonl').read_text().splitlines()]
                    native=[e['event'] for e in events if e.get('event',{}).get('kind')==10 and e['event']['order_attribute']==1]
                    assert {e['order_type'] for e in native}=={0,1,3}
                    assert all(e['price']==3000 for e in native)
                    print(json.dumps({'protocol':protocol,'scenario':scenario,'native_market_attribute':True,'both_venues':True,'day_fak_fok':True,'native_risk_price':True,'factual_fills':6,'reconcile':True}));return
                # Full split close, native type mapping, duplicate trades and exact dates.
                receipt=submit('close',5,'fak');assert receipt['all_submitted_locally']
                assert [(c['offset'],c['lots']) for c in receipt['children']]==[('close_today',2),('close_yesterday',3)]
                positions=state()['positions'];assert next(p for p in positions if p['symbol']=='cu2612')['position']['today']==0
                opened=submit(lots=2);key=opened['children'][0]['key'];send({'op':'cancel','key':key});wait('cancel_submitted_locally')
                assert next(o for o in state()['orders'] if o['key']==key)['status']=='cancelled'
                # Native checkOrder false is a certain rejection and never sends.
                rejected=submit(ticks=25000);assert rejected['children'][0]['outcome']=='rejected_locally'
                failed=submit(ticks=25010);failed_key=failed['children'][0]['key']
                assert next(o for o in state()['orders'] if o['key']==failed_key)['status']=='rejected'
                # Cancel-error callbacks are audit evidence, not a correlated OMS transition.
                cancel_error=submit(ticks=25015)['children'][0]['key'];send({'op':'cancel','key':cancel_error});wait('cancel_submitted_locally')
                assert next(o for o in state()['orders'] if o['key']==cancel_error)['cancel_pending']
                # Native FOK is integer 3, unlike CTP's adapter-owned TIF tag.
                assert submit(tif='fok')['all_submitted_locally']
                send({'op':'reconcile'});wait('reconciling');restored=wait('reconciled')['state'];assert restored['ready']
                assert next(p for p in restored['positions'] if p['symbol']=='cu2612' and p['side']=='sell')['position']['today']==2
                if protocol=='xtcp':
                    assert submit(ticks=25025)['all_submitted_locally']
                    disconnected=full_state();assert not disconnected['transport_ready'] and not disconnected['can_trade']
                    send({'op':'submit','intent':{'venue':'SHFE','symbol':'cu2612','side':'sell','offset':'open','lots':1,'price_ticks':20000,'time_in_force':'day'}})
                    wait('command_rejected')
                    send({'op':'reconcile'});wait('reconciling');wait('reconciled');assert full_state()['can_trade']
                uncertain=submit(ticks=25005);assert uncertain['children'][0]['outcome']=='uncertain';assert not state()['ready']
                send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                events=[json.loads(l) for l in (temp/'events.jsonl').read_text().splitlines()]
                assert not any(e.get('type')=='trading_blocked' for e in events)
                assert any(e.get('event',{}).get('commission')==1.25 for e in events)
                assert 'fixture-only' not in (temp/'events.jsonl').read_text()
                assert any(e.get('event',{}).get('trade_id',0)>(1<<32) for e in events)
                print(json.dumps({'bridge':'native 1.502.53.182 header ABI + synthetic SDK','protocol':protocol,'scenario':scenario,'network_connections':0,'caught_up':True,'manual_ref_zero':True,'shfe_ine_identity_isolation':True,'long_order_and_trade_ids':True,'split_close':True,'native_cancel':True,'fak_fok':True,'reconcile':True,'local_and_async_reject':True,'cancel_error_audit':True,'uncertain_send':True,'journal_events':len(events)}))
            finally:
                if process.poll() is None:process.kill();process.wait()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sdk-dir',type=Path,required=True)
    parser.add_argument('--binary',type=Path,required=True)
    args=parser.parse_args()
    for protocol in ('tcp','udp','xtcp'):run_fixture(args,protocol)
    for protocol in ('tcp','udp','xtcp'):run_fixture(args,protocol,'market')
    run_fixture(args,'xtcp','market_no_limits')
    for protocol in ('udp','xtcp'):run_fixture(args,protocol,'no_port')
    run_fixture(args,'xtcp','no_xtcp')
    run_fixture(args,'udp','ack_timeout')
    run_fixture(args,'xtcp','public_simulation')
    run_fixture(args,'xtcp','late_xtcp')
    run_fixture(args,'udp','account_history')
    run_fixture(args,'udp','reject_history')
    run_fixture(args,'xtcp','retained_xtcp')
    run_fixture(args,'xtcp','public_initial_position')
    for scenario in ('read_only','close_only','multi_login','terminal_first','partial_cancel_first','conflicting_fill','malformed_report','cancel_timeout','cancel_error_timeout'):
        run_fixture(args,'udp',scenario)
    if hasattr(os,'sched_getaffinity') and len(os.sched_getaffinity(0))>=2:
        run_fixture(args,'tcp','affinity')

if __name__=='__main__':main()
