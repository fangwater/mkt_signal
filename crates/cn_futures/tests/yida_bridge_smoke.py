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
    YDAccountExchangeInfo ax[2]{}; YDAccountProductInfo ap[2]{}; YDAccountInstrumentInfo ai[2]{};
    YDExtendedPosition positions[8]{};
    std::map<std::pair<int,int>,YDExtendedOrder> orders;
    std::vector<YDExtendedTrade> trades;
    int next_reference=101; bool initialized=false;
    int index(const YDInstrument* p){return p==&instruments[0]?0:1;}
    void fill(YDExtendedOrder& o,int lots){
        const int k=index(o.m_pInstrument);YDExtendedTrade t{};
        t.m_pInstrument=o.m_pInstrument;t.m_pAccount=&account;
        t.Direction=o.Direction;t.OffsetFlag=o.OffsetFlag;t.HedgeFlag=o.HedgeFlag;
        t.OrderRef=o.OrderRef;t.OrderSysID=o.OrderSysID;t.TradeID=1000+trades.size();t.Price=o.Price;t.Volume=lots;t.Commission=1.25;
        const int side=o.OffsetFlag==YD_OF_Open?o.Direction:1-o.Direction;
        const int date=o.OffsetFlag==YD_OF_CloseYesterday?1:0;
        auto& pos=positions[k*4+side*2+date];
        if(o.OffsetFlag==YD_OF_Open)pos.Position+=lots;else {pos.Position-=lots;pos.CloseFrozen-=lots;}
        o.TradeVolume+=lots;trades.push_back(t);
        listener->notifyTrade(&t,o.m_pInstrument,&account);listener->notifyTrade(&t,o.m_pInstrument,&account);
    }
public:
    Fixture(){
        std::strcpy(account.AccountID,"fixture");account.LoginCount=1;account.TradingRight=YD_TR_Allow;
        cash.m_pAccount=&account;cash.Available=1000000;
        for(int k=0;k<2;++k){
            std::strcpy(exchanges[k].ExchangeID,k?"INE":"SHFE");exchanges[k].UseTodayPosition=true;exchanges[k].ExchangeRef=k+1;
            std::strcpy(products[k].ProductID,k?"sc":"cu");products[k].m_pExchange=&exchanges[k];
            std::strcpy(instruments[k].InstrumentID,k?"sc2612":"cu2612");instruments[k].m_pExchange=&exchanges[k];instruments[k].m_pProduct=&products[k];
            instruments[k].ProductClass=YD_PC_Futures;instruments[k].Tick=.2;instruments[k].Multiple=5;instruments[k].ExpireDate=20261220;
            instruments[k].MinLimitOrderVolume=1;instruments[k].MaxLimitOrderVolume=100;
            ax[k].m_pAccount=&account;ax[k].m_pExchange=&exchanges[k];ap[k].m_pAccount=&account;ap[k].m_pProduct=&products[k];ai[k].m_pAccount=&account;ai[k].m_pInstrument=&instruments[k];
            for(int side=0;side<2;++side)for(int date=0;date<2;++date){
                auto& p=positions[k*4+side*2+date];p.m_pAccountInstrumentInfo=&ai[k];
                p.PositionDate=date?YD_PSD_History:YD_PSD_Today;p.PositionDirection=side?YD_PD_Short:YD_PD_Long;
                p.HedgeFlag=YD_HF_Speculation;p.Position=(!k && !side)?(date?3:2):0;
            }
            // Same system ID and manual reference zero on different exchanges.
            YDExtendedOrder o{};o.m_pAccount=&account;o.m_pInstrument=&instruments[k];o.OrderSysID=55;o.OrderRef=0;
            o.HedgeFlag=YD_HF_Speculation;o.OrderVolume=1;o.Price=.2;o.OrderStatus=YD_OS_Queuing;
            orders[{k,55}]=o;
        }
    }
    bool start(YDListener* l)override{listener=l;listener->notifyEvent(YD_AE_TCPTradeConnected);listener->notifyReadyForLogin(false);return true;}
    bool startExtended(YDListener* l,YDExtendedListener* e)override{extended=e;return start(l);}
    void startDestroy()override{auto* l=listener;if(l){std::thread([this,l]{std::this_thread::sleep_for(std::chrono::milliseconds(5));l->notifyBeforeApiDestroy();delete this;l->notifyAfterApiDestroy();}).detach();}else{delete this;}}
    bool login(const char* u,const char* p,const char* a,const char* auth)override{
        check(std::strcmp(u,"fixture")==0 && std::strcmp(p,"fixture-only")==0 && std::strcmp(a,"fixture-app")==0 && std::strcmp(auth,"fixture-auth")==0);
        listener->notifyLogin(0,next_reference-1,false);
        if(!initialized){initialized=true;listener->notifyFinishInit();}
        listener->notifyCaughtUp();return true;
    }
    void disconnect()override{listener->notifyEvent(YD_AE_TCPTradeDisconnected);listener->notifyEvent(YD_AE_TCPTradeConnected);listener->notifyReadyForLogin(false);}
    int getTradingDay()override{return 20261109;}
    int getNextOrderRef()override{return next_reference++;}
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
    YDQueryResult<YDExtendedPosition>* findExtendedPositions(const YDExtendedPositionFilter* f)override{check(f->HedgeFlag==-1 && f->PositionDate==-1 && !f->pAccount);std::vector<const YDExtendedPosition*> r;for(auto& p:positions)r.push_back(&p);return new Rows<YDExtendedPosition>(r);}
    unsigned findExtendedPositions(const YDExtendedPositionFilter*,unsigned,const YDExtendedPosition*[])override{return 0;}
    YDQueryResult<YDExtendedOrder>* findOrders(const YDOrderFilter* f)override{check(f->StartTime==-1 && f->EndTime==-1 && f->YDOrderFlags==-1 && !f->pAccount);std::vector<const YDExtendedOrder*> r;for(auto& pair:orders)r.push_back(&pair.second);return new Rows<YDExtendedOrder>(r);}
    unsigned findOrders(const YDOrderFilter*,unsigned,const YDExtendedOrder*[])override{return 0;}
    YDQueryResult<YDExtendedTrade>* findTrades(const YDTradeFilter* f)override{check(f->StartTime==-1 && f->EndTime==-1 && !f->pAccount);std::vector<const YDExtendedTrade*> r;for(auto& t:trades)r.push_back(&t);return new Rows<YDExtendedTrade>(r);}
    unsigned findTrades(const YDTradeFilter*,unsigned,const YDExtendedTrade*[])override{return 0;}
    bool checkOrder(YDInputOrder* o,const YDInstrument*,const YDAccount* a=nullptr)override{
        check(!a && o->OrderRef>100 && o->HedgeFlag==YD_HF_Speculation && o->YDOrderFlag==YD_YOF_Normal && o->ConnectionSelectionType==YD_CS_Any);
        check(o->OrderType==YD_ODT_Limit || o->OrderType==YD_ODT_FAK || o->OrderType==YD_ODT_FOK);
        check(o->OffsetFlag==YD_OF_Open || o->OffsetFlag==YD_OF_CloseToday || o->OffsetFlag==YD_OF_CloseYesterday);
        if(o->Price==5000){o->ErrorNo=40;return false;}return true;
    }
    bool insertOrder(YDInputOrder* p,const YDInstrument* i,const YDAccount* a=nullptr)override{
        check(!a);if(p->Price==5001){p->ErrorNo=80;return false;}
        if(p->Price==5002){YDInputOrder e=*p;e.ErrorNo=25;listener->notifyFailedOrder(&e,i,&account);return true;}
        if(p->Price==5004){listener->notifyEvent(YD_AE_ServerRestarted);return true;}
        YDOrder pending{};static_cast<YDInputOrder&>(pending)=*p;pending.OrderStatus=YD_OS_Accepted;
        listener->notifyOrder(&pending,i,&account); // Server accepted; exchange system ID still unavailable.
        YDExtendedOrder o{};static_cast<YDInputOrder&>(o)=*p;o.OrderSysID=p->OrderRef+1000;
        o.m_pInstrument=i;o.m_pAccount=&account;o.OrderStatus=YD_OS_Queuing;
        auto key=std::make_pair(index(i),o.OrderSysID);orders[key]=o;auto& stored=orders.at(key);
        if(p->OffsetFlag!=YD_OF_Open)positions[index(i)*4+(1-p->Direction)*2+(p->OffsetFlag==YD_OF_CloseYesterday?1:0)].CloseFrozen+=p->OrderVolume;
        if(p->OrderType!=YD_ODT_Limit){fill(stored,p->OrderVolume);stored.OrderStatus=YD_OS_AllTraded;}
        listener->notifyOrder(&stored,i,&account);return true;
    }
    bool cancelOrder(YDCancelOrder* p,const YDExchange* x,const YDAccount* a=nullptr)override{
        check(!a && p->OrderSysID>0 && p->ConnectionSelectionType==YD_CS_Any && p->YDOrderFlag==YD_YOF_Normal);
        auto& o=orders.at({x==&exchanges[0]?0:1,p->OrderSysID});
        if(o.Price==5003){YDFailedCancelOrder e{};e.OrderSysID=o.OrderSysID;e.ErrorNo=31;listener->notifyFailedCancelOrder(&e,x,&account);return true;}
        if(o.OrderVolume-o.TradeVolume>0)fill(o,1);
        if(o.OffsetFlag!=YD_OF_Open)positions[index(o.m_pInstrument)*4+(1-o.Direction)*2+(o.OffsetFlag==YD_OF_CloseYesterday?1:0)].CloseFrozen-=o.OrderVolume-o.TradeVolume;
        o.OrderStatus=YD_OS_Canceled;listener->notifyOrder(&o,o.m_pInstrument,&account);return true;
    }
    // GENERATED_STUBS
};
extern "C" YDExtendedApi* makeYDExtendedApi(const char*){return new Fixture;}
extern "C" const char* getYDVersion(){return "1.188.78.0";}
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

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sdk-dir',type=Path,required=True)
    parser.add_argument('--binary',type=Path,required=True)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[3]
    with tempfile.TemporaryDirectory(prefix='mkt-yida-fixture-') as directory:
        temp=Path(directory);source=temp/'fixture.cpp';library=temp/'fixture.so'
        source.write_text(fixture_source(args.sdk_dir))
        subprocess.run(['c++','-std=c++17','-shared','-fPIC','-pthread','-isystem',str(args.sdk_dir),str(source),'-o',str(library)],check=True)
        native_config=temp/'server.cfg'
        native_config.write_text('TradingServerIP=127.0.0.1\nTradingServerPort=1\nUDPTrading=no\nConnectTCPMarketData=no\nReceiveUDPMarketData=no\nRecalcMode=off\n')
        config=(root/'config/yida_trade.example.toml').read_text()
        for old,new in [('/opt/yida/1.188.78.0/libyd.so',str(library)),('/opt/yida/trade-server.cfg',str(native_config)),('/tmp/mkt_yida_sim/flow',str(temp/'flow')),('/tmp/mkt_yida_sim/events.jsonl',str(temp/'events.jsonl')),('cash_budget_per_open_lot = 250000.0','cash_budget_per_open_lot = 10000.0'),('max_order_lots = 1','max_order_lots = 10'),('max_position_lots_per_side = 2','max_position_lots_per_side = 20'),('max_working_lots = 2','max_working_lots = 20'),('max_actions_per_second = 2','max_actions_per_second = 100'),('enable_fak_fok = false','enable_fak_fok = true')]:config=config.replace(old,new)
        config_path=temp/'config.toml';config_path.write_text(config)
        environment=dict(os.environ,YIDA_USER_ID='fixture',YIDA_PASSWORD='fixture-only',YIDA_APP_ID='fixture-app',YIDA_AUTH_CODE='fixture-auth')
        with open(temp/'stderr.log','w+') as stderr:
            process=subprocess.Popen([str(args.binary.resolve()),'--config',str(config_path),'--connect','--execute'],env=environment,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True)
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
            def state():send({'op':'state'});return wait('state')['state']
            def submit(offset='open',lots=1,tif='day',ticks=20000):
                send({'op':'submit','intent':{'venue':'SHFE','symbol':'cu2612','side':'sell','offset':offset,'lots':lots,'price_ticks':ticks,'time_in_force':tif}})
                return wait('submit_receipt')['receipt']
            try:
                initial=wait('reconciled')['state'];assert initial['ready']
                assert len([o for o in initial['orders'] if o['key']['order_ref']=='0'])==2
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
                # Cancel errors keep the reservation and permit a retry.
                cancel_error=submit(ticks=25015)['children'][0]['key'];send({'op':'cancel','key':cancel_error});wait('cancel_submitted_locally')
                assert not next(o for o in state()['orders'] if o['key']==cancel_error)['cancel_pending']
                # Native FOK is integer 3, unlike CTP's adapter-owned TIF tag.
                assert submit(tif='fok')['all_submitted_locally']
                send({'op':'reconcile'});wait('reconciling');restored=wait('reconciled')['state'];assert restored['ready']
                assert next(p for p in restored['positions'] if p['symbol']=='cu2612' and p['side']=='sell')['position']['today']==2
                uncertain=submit(ticks=25005);assert uncertain['children'][0]['outcome']=='uncertain';assert not state()['ready']
                send({'op':'shutdown'});wait('runtime_stopped');assert process.wait(timeout=10)==0
                events=[json.loads(l) for l in (temp/'events.jsonl').read_text().splitlines()]
                assert not any(e.get('type')=='trading_blocked' for e in events)
                assert any(e.get('event',{}).get('commission')==1.25 for e in events)
                assert 'fixture-only' not in (temp/'events.jsonl').read_text()
                print(json.dumps({'bridge':'native YDApi header ABI + synthetic SDK','network_connections':0,'caught_up':True,'manual_ref_zero':True,'shfe_ine_identity_isolation':True,'split_close':True,'native_cancel':True,'fak_fok':True,'reconcile':True,'local_and_async_reject':True,'cancel_reject':True,'uncertain_send':True,'journal_events':len(events)}))
            finally:
                if process.poll() is None:process.kill();process.wait()

if __name__=='__main__':main()
