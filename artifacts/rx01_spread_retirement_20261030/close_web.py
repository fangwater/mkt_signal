from pathlib import Path
import hashlib,json,os,subprocess,shutil
stage=Path.home()/"deploy-staging/rx01-retirement-20261030"
backup=Path((stage/"archive_path").read_text())
site=Path("/etc/nginx/sites-available/crypto_proxy_4191.conf")
assert site.read_bytes()==(backup/"nginx_site.before").read_bytes(),"active gateway changed since audit"
locations=Path.home()/"nginx_locations.txt"
assert locations.read_bytes()==(backup/"nginx_locations.before").read_bytes(),"route map changed since audit"
snippet=stage/"payload/cta_rx01_retired.conf"
assert hashlib.sha256(snippet.read_bytes()).hexdigest()=="a34353907195038fe24a36c821ceb0583b48d9cb62e4bb6eebb388817de8da6b"
def command(*args):subprocess.run(args,check=True)
command("sudo","-n","install","-m","644",str(snippet),"/etc/nginx/snippets/cta_rx01_retired.conf")
command("sudo","-n","install","-m","644",str(stage/"nginx_site.candidate"),str(site)+".rx01-next")
command("sudo","-n","mv",str(site)+".rx01-next",str(site))
try:command("sudo","-n","nginx","-t")
except BaseException:
 command("sudo","-n","install","-m","644",str(backup/"nginx_site.before"),str(site));raise
map_next=locations.with_suffix(".rx01-next")
shutil.copyfile(stage/"nginx_locations.candidate",map_next);map_next.chmod(0o600);map_next.replace(locations)
command("sudo","-n","systemctl","reload","nginx")
root=Path.home()/"crypto_cta_manager"
assert os.readlink(root/"webroot")== (backup/"manager_webroot.before").read_text()
next_link=root/"webroot.rx01-next"
next_link.symlink_to("web-releases/rx01-retired-20261030");next_link.replace(root/"webroot")
print(json.dumps({"old_routes":"410","gateway":"validated_and_reloaded","manager_frontend":"rx01-retired-20261030","manager_restarted":False}))
