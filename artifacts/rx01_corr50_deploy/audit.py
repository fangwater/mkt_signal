import hashlib,json,os
from pathlib import Path

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None

def snapshot():
    home=Path.home(); root=home/'binance-cta-spread-rx01'
    processes={}
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            comm=(proc/'comm').read_text().strip()
            if comm in {'python3','trade_engine','exec-pre-trade','persist_manager','viz_server','cta_spread_fact','cta_spread_sign','cta_perp_factor','cta_perp_signal'} or 'account_mon' in comm:
                cwd=os.readlink(proc/'cwd')
                if cwd.startswith(str(home)+'/'):
                    cpus=next((line.split(':',1)[1].strip() for line in (proc/'status').read_text().splitlines() if line.startswith('Cpus_allowed_list:')),None)
                    processes[proc.name]={'comm':comm,'cwd':cwd,'cpus':cpus}
        except OSError: pass
    protected=[root/'env.sh',root/'run/cta_spread_position_flags.json',home/'binance_exec_trade06/env.sh',home/'binance_exec_trade06/config/trade_engine.toml']
    return {'processes':processes,'files':{str(p):digest(p) for p in protected}}

if __name__=='__main__': print(json.dumps(snapshot(),indent=2))
