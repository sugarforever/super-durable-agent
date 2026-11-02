import { openSuperAgent } from '../../dist/runtime/open.js';
try {
  const runtime = await openSuperAgent({ directory: process.argv[2],
    preflight: process.argv[3] !== 'hold' ? undefined : async (engine) => {
      // Persist the intent while scheduling is paused, then let the parent kill us.
      await engine.receive({ sourceId: 'cli', externalId: 'crash', revision: '1',
        actor: { id: 'alice', kind: 'human', trusted: true }, threads: ['crash'], body: 'durable',
        receivedAt: 1, occurredAt: 1 });
      console.log('ready');
      await new Promise(() => { setInterval(() => {}, 1000); });
    },
  });
  await runtime.close();
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
