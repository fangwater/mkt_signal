from pathlib import Path
import hashlib
import json
import subprocess

env = Path.home() / 'binance-cta-perp-rx03'
nginx = Path('/etc/nginx/sites-available/crypto_proxy_4191.conf')
before = nginx.read_bytes()
text = before.decode()
assert '/cta/binance-cta-perp-rx03/' not in text
anchor = '    location = /cta/binance-cta-perp-rx02 {'
assert text.count(anchor) == 1
block = '''    location = /cta/binance-cta-perp-rx03 {
        return 302 /cta/binance-cta-perp-rx03/$is_args$args;
    }
    location = /cta/binance-cta-perp-rx03/config {
        return 302 /cta/binance-cta-perp-rx03/config/$is_args$args;
    }
    location /cta/binance-cta-perp-rx03/config/ {
        proxy_pass http://127.0.0.1:19187/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        add_header Cache-Control "no-store";
    }
    location /cta/binance-cta-perp-rx03/ {
        proxy_pass http://127.0.0.1:10197/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        add_header Cache-Control "no-store";
    }

'''
backup = env / 'run/nginx.before.conf'
assert not backup.exists()
backup.write_bytes(before)
backup.chmod(0o600)
after = env / 'run/nginx.after.conf'
after.write_text(text.replace(anchor, block + anchor))
after.chmod(0o600)
assert nginx.read_bytes() == before
temp = str(nginx) + '.rx03.tmp'
subprocess.run(['sudo', '-n', 'install', '-m', '644', str(after), temp], check=True)
subprocess.run(['sudo', '-n', 'mv', temp, str(nginx)], check=True)
try:
    subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
except BaseException:
    subprocess.run(['sudo', '-n', 'install', '-m', '644', str(backup), str(nginx)], check=True)
    subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
    raise
print(json.dumps({'route': '/cta/binance-cta-perp-rx03/', 'nginx_sha256': hashlib.sha256(nginx.read_bytes()).hexdigest()}))
