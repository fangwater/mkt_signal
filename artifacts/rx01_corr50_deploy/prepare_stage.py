from pathlib import Path
import hashlib,json,shutil
root=Path.cwd(); stage=root/'artifacts/rx01_corr50_deploy/stage'
files=['scripts/cta_spread_config_server.py','scripts/cta_spread_factor_config.py','scripts/cta_spread_deployment.py','scripts/cta_spread_dashboard.py','scripts/cta_spread_signal_history.py','scripts/factor_validation_allowlist.txt','web/cta_spread_config/index.html','web/cta_spread/index.html','web/cta_spread_signals/panel.js','config/cta_spread.json','config/cta_spread_symbol_factors.json','config/cta_spread_factor_model_1m_pub.toml','config/cta_spread_deployment.json']
for relative in files:
    target=stage/relative; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(root/relative,target)
config=json.loads((stage/'config/cta_spread.json').read_text())
config['max_pos_u']=json.loads((root/'artifacts/rx01_corr50_deploy/before/cta_spread.json').read_text())['max_pos_u']
assert config['max_pos_u']==100 and not config['enabled']
(stage/'config/cta_spread.json').write_text(json.dumps(config,indent=2)+'\n')
for binary in ['cta_spread_signal','cta_spread_factor_model_1m_pub']:
    shutil.copyfile(root/'target/release'/binary,stage/binary); files.append(binary)
manifest={name:hashlib.sha256((stage/name).read_bytes()).hexdigest() for name in files}
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('staged files:',len(manifest))
