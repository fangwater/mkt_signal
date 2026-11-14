#!/usr/bin/env python3
"""Exercise the official-header MD bridge without opening any network connection."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib

MOCK = r'''
#include "ThostFtdcMdApi.h"
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
static void check(bool value){if(!value)throw std::runtime_error("MD fixture request mismatch");}
class Fixture final:public CThostFtdcMdApi {
    CThostFtdcMdSpi* spi=nullptr;
    int subscriptions=0;
public:
    void Release() override {delete this;}
    void RegisterSpi(CThostFtdcMdSpi* value) override {spi=value;}
    void RegisterFront(char* value) override {check(std::strcmp(value,"tcp://127.0.0.1:1")==0);}
    void Init() override {spi->OnFrontConnected();}
    int ReqUserLogin(CThostFtdcReqUserLoginField* p,int id) override {
        check(std::strcmp(p->BrokerID,"TEST")==0 && std::strcmp(p->UserID,"test")==0 && std::strcmp(p->Password,"fixture-only")==0);
        std::ofstream(std::getenv("MD_FIXTURE_COUNTER"),std::ios::app)<<"login\n";
        CThostFtdcRspUserLoginField r{};std::strcpy(r.TradingDay,"20261111");
        CThostFtdcRspInfoField error{};
        if(std::getenv("MD_FIXTURE_LOGIN_ERROR"))error.ErrorID=3;
        spi->OnRspUserLogin(&r,&error,id,true);return 0;
    }
    int SubscribeMarketData(char** symbols,int count) override {
        check(count==2);++subscriptions;
        for(int i=0;i<count;++i){
            CThostFtdcSpecificInstrumentField s{};std::strcpy(s.InstrumentID,symbols[i]);spi->OnRspSubMarketData(&s,nullptr,subscriptions,i==count-1);
            CThostFtdcDepthMarketDataField t{};std::strcpy(t.InstrumentID,symbols[i]);
            std::strcpy(t.ExchangeID,std::strcmp(symbols[i],"IF2612")==0?"CFFEX":"SHFE");
            std::strcpy(t.TradingDay,"20261111");std::strcpy(t.ActionDay,"20261111");std::strcpy(t.UpdateTime,"09:31:00");
            t.UpdateMillisec=500;t.LastPrice=4000;t.Volume=100;t.BidPrice1=3999.8;t.AskPrice1=4000.2;t.BidVolume1=2;t.AskVolume1=3;
            t.BidPrice5=3999;t.AskPrice5=4001;t.BidVolume5=4;t.AskVolume5=5;t.OpenInterest=1000;t.Turnover=120000000;
            spi->OnRtnDepthMarketData(&t);
        }
        if(subscriptions==1){spi->OnFrontDisconnected(0x1001);spi->OnFrontConnected();}
        return 0;
    }
    // GENERATED_STUBS
};
const char* CThostFtdcMdApi::GetApiVersion(){return "v6.7.11_fixture";}
CThostFtdcMdApi* CThostFtdcMdApi::CreateFtdcMdApi(const char*,bool udp,bool multicast,bool production){check(!udp && !multicast && !production);return new Fixture;}
'''

def fixture_source(sdk):
    header=(sdk/'ThostFtdcMdApi.h').read_text(errors='replace')
    implemented=set(re.findall(r'\b(\w+)\([^;{}]*\)\s*override\s*\{',MOCK))
    stubs=[]
    for signature in re.findall(r'virtual\s+([^;\n]+?)\s*=\s*0\s*;',header):
        name=re.search(r'(\w+)\s*\(',signature).group(1)
        if name not in implemented:
            result='' if signature.startswith('void ') else 'return -9901;' if signature.startswith('int ') else 'return {};'
            stubs.append(signature+' override { '+result+' }')
    return MOCK.replace('// GENERATED_STUBS','\n'.join(stubs))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sdk-dir',type=Path,required=True)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument("--placement-config",type=Path,required=True,help="explicit CPU/NUMA placement for the authorized external host")
    args=parser.parse_args()
    placement=tomllib.loads(args.placement_config.read_text())
    required={"numa_node","business_cpu","sdk_cpus","background_cpus"}
    if set(placement)!=required: raise ValueError("placement config must contain exactly NUMA node and three CPU domains")
    placement_table="[placement]\n"+"\n".join(k+" = "+json.dumps(placement[k]) for k in ["numa_node","business_cpu","sdk_cpus","background_cpus"])+"\n"
    root=Path(__file__).resolve().parents[3]
    with tempfile.TemporaryDirectory(prefix='mkt-ctp-md-fixture-') as directory:
        temp=Path(directory);source=temp/'fixture.cpp';library=temp/'fixture.so'
        source.write_text(fixture_source(args.sdk_dir))
        subprocess.run(['c++','-std=c++17','-shared','-fPIC','-isystem',str(args.sdk_dir),str(source),'-o',str(library)],check=True)
        template=(root/'config/ctp_market_data.example.toml').read_text()
        template=template[:template.index("[placement]")]+placement_table
        for failure in [False,True]:
            case=temp/('login-error' if failure else 'reconnect');case.mkdir()
            config=template.replace('/opt/ctp/6.7.11/libthostmduserapi_se.so',str(library)).replace('/tmp/mkt_ctp_md/flow',str(case/'flow')).replace('/tmp/mkt_ctp_md/market_data.jsonl',str(case/'events.jsonl')).replace('broker_id = "REPLACE"','broker_id = "TEST"')
            path=case/'config.toml';path.write_text(config)
            environment=dict(os.environ,CTP_USER_ID='test',CTP_PASSWORD='fixture-only',MD_FIXTURE_COUNTER=str(case/'counter.txt'))
            if failure:environment['MD_FIXTURE_LOGIN_ERROR']='1'
            else:environment.pop('MD_FIXTURE_LOGIN_ERROR',None)
            run=subprocess.run([str(args.binary.resolve()),'--config',str(path),'--connect','--duration-seconds','2'],cwd=case,env=environment,capture_output=True,text=True,timeout=15)
            events=[json.loads(line) for line in run.stdout.splitlines()]
            if failure:
                assert run.returncode!=0
                assert (case/'counter.txt').read_text().splitlines()==['login']
                assert any(e['type']=='stream_failed' for e in events)
            else:
                assert run.returncode==0,run.stderr
                ticks=[e['tick'] for e in events if e['type']=='tick']
                assert len(ticks)==4
                assert {t['connection_generation'] for t in ticks}=={1,2}
                assert {t['venue'] for t in ticks}=={'CFFEX','SHFE'}
                assert all(t['received_us']>0 and len(t['bid'])==5 and t['ask_volume'][4]==5 for t in ticks)
                assert (case/'counter.txt').read_text().splitlines()==['login','login']
                assert events[-1]['ready'] and events[-1]['ticks']==4
            assert 'fixture-only' not in (case/'events.jsonl').read_text()
        print(json.dumps({'official_header_bridge':True,'native_network_connections':0,'login':True,'subscribe':True,'five_levels':True,'disconnect_gap':True,'relogin_and_resubscribe':True,'bad_password_attempts':1,'credentials_redacted':True}))

if __name__=='__main__':main()
