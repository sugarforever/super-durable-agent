#!/usr/bin/env node
import { resolve } from 'node:path';
import { openSuperAgent } from './runtime/open.js';
import type { NormalizedEvent, RuntimeMetadata } from './domain/model.js';

const help = `Super Durable Agent

Usage: super-durable-agent [--directory PATH] <command>

Commands:
  init                Initialize a durable agent directory.
  receive             Receive one normalized event as JSON from standard input.
  list <kind>         List persisted records of one kind.
  show <kind> <id>    Show one persisted record.

Directory: --directory / --data-dir, SUPER_AGENT_DIRECTORY, or .super-durable-agent
No model credentials are required for these commands.
`;

async function main(): Promise<void> {
  const positional: string[] = [];
  let directory = process.env.SUPER_AGENT_DIRECTORY ?? '.super-durable-agent';
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === '--help' || arg === '-h') { process.stdout.write(help); return; }
    if (arg === '--directory' || arg === '--data-dir') {
      const value = args[++index];
      if (!value || value.startsWith('--')) throw new Error(`${arg} requires a path`);
      directory = value;
    } else if (arg.startsWith('-')) throw new Error(`Unknown option: ${arg}`);
    else positional.push(arg);
  }
  const [command, kind, id] = positional;
  if (!command) { process.stdout.write(help); return; }
  const expected = command === 'init' || command === 'receive' ? 1 : command === 'list' ? 2 : command === 'show' ? 3 : undefined;
  if (expected === undefined) throw new Error(`Unknown command: ${command}`);
  if (positional.length !== expected) throw new Error(`Invalid arguments for ${command}. Use --help.`);
  let event: NormalizedEvent | undefined;
  if (command === 'receive') {
    const chunks: Buffer[] = [];
    let bytes = 0;
    for await (const chunk of process.stdin) {
      const data = Buffer.from(chunk);
      bytes += data.length;
      if (bytes > 1_048_576) throw new Error('Input exceeds the 1 MiB limit');
      chunks.push(data);
    }
    event = JSON.parse(Buffer.concat(chunks).toString('utf8')) as NormalizedEvent;
  }
  const runtime = await openSuperAgent({ directory });
  try {
    let result: unknown;
    switch (command) {
      case 'init':
        result = { directory: resolve(directory), instanceId: (await runtime.engine.store.read<RuntimeMetadata>('metadata', 'runtime'))!.instanceId };
        break;
      case 'receive':
        result = await runtime.receive(event!);
        if ((result as { disposition: string }).disposition === 'backpressure') process.exitCode = 1;
        break;
      case 'list': result = await runtime.list(kind); break;
      case 'show':
        result = await runtime.engine.store.read(kind, id);
        if (result === undefined) throw new Error(`Record not found: ${kind}/${id}`);
        break;
    }
    process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  } finally { await runtime.close(); }
}

main().catch((error: unknown) => {
  process.stderr.write((error instanceof Error ? error.message : String(error)) + '\n');
  process.exitCode = 1;
});
