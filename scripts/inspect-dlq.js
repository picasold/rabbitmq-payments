process.env.ALLOW_CLOSE = '1';
// Melihat isi Dead Letter Queue tanpa menghapus pesannya.
// Jalankan: npm run dlq:inspect
import { QUEUES } from '../src/config.js';
import { connectWithRetry, assertTopology } from '../src/topology.js';

const connection = await connectWithRetry();
const channel = await connection.createChannel();
await assertTopology(channel);

const info = await channel.checkQueue(QUEUES.DLQ);
console.log(`\nQueue "${QUEUES.DLQ}" berisi ${info.messageCount} pesan\n`);

const seen = [];
for (let i = 0; i < info.messageCount; i++) {
  const msg = await channel.get(QUEUES.DLQ, { noAck: false });
  if (!msg) break;
  const body = JSON.parse(msg.content.toString());
  seen.push(msg);
  console.log(`${i + 1}. ${body.order_id}  Rp${Number(body.amount).toLocaleString('id-ID')}`);
  console.log(`   payment_id : ${body.payment_id}`);
  console.log(`   alasan     : ${msg.properties.headers['x-dead-reason']}`);
  console.log(`   percobaan  : ${msg.properties.headers['x-retry-attempts']}x`);
  console.log(`   waktu      : ${msg.properties.headers['x-dead-lettered-at']}\n`);
}

// nack dengan requeue=true -> pesan dikembalikan, DLQ tetap utuh
for (const msg of seen) channel.nack(msg, false, true);

await new Promise((r) => setTimeout(r, 300));
await connection.close();
process.exit(0);
