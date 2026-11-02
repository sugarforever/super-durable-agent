import { readdir, readFile, stat } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';

const root = process.cwd();
const files = ['README.md'];
async function collect(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const name = join(directory, entry.name);
    if (entry.isDirectory()) await collect(name);
    else if (entry.name.endsWith('.md')) files.push(name);
  }
}
await collect('docs');
const errors = [];
for (const file of files) {
  const body = await readFile(file, 'utf8');
  if (/\p{Script=Han}/u.test(body)) errors.push(`${file}: documentation must be in English`);
  for (const [, target] of body.matchAll(/\]\(([^)]+)\)/g)) {
    if (/^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith('#')) continue;
    const local = resolve(dirname(file), target.split('#')[0]);
    if (!local.startsWith(root + '/') && local !== root) {
      errors.push(`${file}: link escapes repository: ${target}`);
      continue;
    }
    try { await stat(local); }
    catch { errors.push(`${file}: missing local link: ${target}`); }
  }
}
if (errors.length) throw new Error(errors.join('\n'));
console.log(`PASS: ${files.length} English documents; local links resolve`);
