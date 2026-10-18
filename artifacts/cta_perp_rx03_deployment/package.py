from pathlib import Path
import hashlib,json,shutil,subprocess,tarfile
root=Path('/home/fanghaizhou/mkt_signal')
evidence=root/'artifacts/cta_perp_rx03_deployment'
assert subprocess.check_output(['git','branch','--show-current'],cwd=root,text=True).strip()=='arbmm'
commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
assert subprocess.check_output(['git','rev-parse','origin/arbmm'],cwd=root,text=True).strip()==commit
stage=Path('/tmp/cta_perp_rx03_payload'); stage.mkdir(exist_ok=False)
files=[*root.glob('scripts/start_cta_perp*.sh'),*root.glob('scripts/stop_cta_perp*.sh'),*root.glob('scripts/cta_perp*.py'),root/'scripts/process_match_lib.sh',root/'scripts/factor_validation_allowlist.txt',root/'config/iceoryx2.toml',root/'config/cta_perp_replay.json',*root.glob('web/cta_perp*/*'),*root.glob('web/vendor/*')]
for p in files:
 if p.is_file():
  dest=stage/p.relative_to(root);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest)
for p in (root/'config/cta_perp_rx03').glob('*'):shutil.copy2(p,stage/'config'/p.name)
for name in ('cta_perp_signal','cta_perp_factor_model_1m_pub'):shutil.copy2(root/'target/release'/name,stage/name)
(stage/'env.sh').write_text("export IPC_NAMESPACE='binance_cta_perp_rx03'\nexport CTA_PERP_MODEL_SCOPE='binance_cta_perp_rx03'\nexport CTA_PERP_FACTOR_PROCESS_NAME='cta_perp_factor_binance_cta_perp_rx03'\nexport CTA_POSITION_STRATEGY_PREFIX='binance_cta_perp_rx03_'\nexport CTA_VIRTUAL_ACCOUNT_NAME='cta_perp_rx03'\nexport CTA_PERP_CONFIG_BIND='127.0.0.1'\nexport CTA_PERP_CONFIG_PORT='19187'\nexport CTA_PERP_DASHBOARD_BIND='127.0.0.1'\nexport CTA_PERP_DASHBOARD_PORT='10197'\nexport CTA_PERP_RUN_FACTOR_PUBLISHER='1'\nexport CTA_PERP_FACTOR_MODEL_1M_CORE='19'\nexport RUST_LOG='info'\n")
(stage/'env.sh').chmod(0o600)
manifest={'base_commit':commit,'branch':'arbmm','env_name':'binance-cta-perp-rx03','publication_enabled':False,'max_pos_u':100,'bindings':34,'symbols':14,'factors':5,'model_scope':'binance_cta_perp_rx03','publisher_core':19,'files':{}}
for p in sorted(stage.rglob('*')):
 if p.is_file():manifest['files'][str(p.relative_to(stage))]=hashlib.sha256(p.read_bytes()).hexdigest()
(stage/'SHA256SUMS').write_text(''.join(v+'  '+k+'\n' for k,v in manifest['files'].items()))
(evidence/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
subprocess.run(['git','diff','--binary','--','src/cta_perp','src/factor_pub/cta_perp_factor_model_1m_pub/app.rs','scripts/start_cta_perp_factor_model_1m_pub.sh','scripts/stop_cta_perp_factor_model_1m_pub.sh'],cwd=root,check=True,stdout=(evidence/'source.patch').open('w'))
shutil.copy2(root/'src/cta_perp/model_service.rs',evidence/'model_service.rs')
with tarfile.open('/tmp/cta_perp_rx03_payload.tar.gz','w:gz') as t:
 for p in sorted(stage.iterdir()):t.add(p,arcname=p.name)
print(json.dumps({'package':'/tmp/cta_perp_rx03_payload.tar.gz','files':len(manifest['files']),'sha256':hashlib.sha256(Path('/tmp/cta_perp_rx03_payload.tar.gz').read_bytes()).hexdigest()}))
