import { build } from 'esbuild';
import { copyFile, mkdir } from 'node:fs/promises';

await mkdir(new URL('../dist/', import.meta.url), { recursive: true });
await build({
  absWorkingDir: new URL('.', import.meta.url).pathname,
  entryPoints: ['src.jsx'],
  bundle: true,
  minify: true,
  outfile: '../dist/app.js',
  target: ['es2020'],
  define: { 'process.env.NODE_ENV': '"production"' },
  legalComments: 'eof',
});
await copyFile(new URL('index.html', import.meta.url), new URL('../dist/index.html', import.meta.url));
