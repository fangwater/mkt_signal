from pathlib import Path
import re
import shutil
import subprocess

stage = Path.home() / 'binance_exec_trade07/run/activation_20261014'
site = Path('/etc/nginx/sites-available/crypto_proxy_4191.conf')
mapping = Path.home() / 'nginx_locations.txt'
snippet = Path('/etc/nginx/snippets/cta_perp_rx03.conf')
before = site.read_text()
mapping_before = mapping.read_text()
assert not snippet.exists()
assert 'binance-cta-perp-rx03' not in mapping_before
blocks = []
for match in re.finditer(r'^    location [^\n]*?/cta/binance-cta-perp-rx03[^\n]*\{', before, re.M):
    pos = match.end()
    depth = 1
    while depth:
        if before[pos] == '{':
            depth += 1
        elif before[pos] == '}':
            depth -= 1
        pos += 1
    blocks.append(before[match.start():pos])
assert len(blocks) == 4
for source, name in [(site, 'gateway_site.before'), (mapping, 'gateway_mapping.before')]:
    shutil.copy2(source, stage / name)
    (stage / name).chmod(0o600)
text = '\n\n'.join(blocks) + '\n'
(stage / 'rx03_signal_gateway.conf').write_text(text)
subprocess.run(['sudo', '-n', 'install', '-m', '644', str(stage / 'rx03_signal_gateway.conf'), str(snippet)], check=True)
after = before.replace(blocks[0], '    include ' + str(snippet) + ';', 1)
for block in blocks[1:]:
    after = after.replace(block, '', 1)
assert before.count('location') == after.count('location') + 4
(stage / 'gateway_site.after').write_text(after)
subprocess.run(['sudo', '-n', 'install', '-m', '644', str(stage / 'gateway_site.after'), str(site) + '.next'], check=True)
assert site.read_text() == before and mapping.read_text() == mapping_before
subprocess.run(['sudo', '-n', 'mv', str(site) + '.next', str(site)], check=True)
try:
    subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
    mapping.write_text(mapping_before + '\n# BEGIN managed: cta perp rx03\n/cta/binance-cta-perp-rx03/ external:' + str(snippet) + '\n# END managed: cta perp rx03\n')
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
except BaseException:
    subprocess.run(['sudo', '-n', 'install', '-m', '644', str(stage / 'gateway_site.before'), str(site)], check=True)
    mapping.write_text(mapping_before)
    raise
print('RX03 signal gateway preserved in nginx_locations.txt via exact existing route snippet')
