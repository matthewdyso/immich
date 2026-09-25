// Run in the built server image against a DISPOSABLE PostgreSQL database.
const assert = require('node:assert/strict');
const { Kysely, sql } = require('kysely');
const { getKyselyConfig } = require('./dist/utils/database.js');
const { SyncCheckpointRepository } = require('./dist/repositories/sync-checkpoint.repository.js');

const config = () => getKyselyConfig({ connectionType: 'url', url: process.env.TEST_DATABASE_URL });
const writer = new Kysely(config());
const reader = new Kysely(config());
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  // Production defines this function in a migration; the disposable database needs a copy.
  await sql`CREATE OR REPLACE FUNCTION immich_uuid_v7(p_timestamp timestamptz DEFAULT clock_timestamp()) RETURNS uuid
    VOLATILE LANGUAGE sql AS $$
      select encode(set_bit(set_bit(overlay(uuid_send(gen_random_uuid()) placing
        substring(int8send(floor(extract(epoch from p_timestamp) * 1000)::bigint) from 3) from 1 for 6), 52, 1), 53, 1), 'hex')::uuid
    $$`.execute(writer);
  await sql`CREATE TABLE IF NOT EXISTS item (id int PRIMARY KEY, "updateId" uuid NOT NULL)`.execute(writer);
  const repo = new SyncCheckpointRepository(reader);

  let release;
  const held = new Promise((resolve) => (release = resolve));
  let marker;
  // An upload or metadata job stamps a row, then takes a while to commit.
  const slowWrite = writer.transaction().execute(async (tx) => {
    const row = await sql`INSERT INTO item VALUES (1, immich_uuid_v7()) RETURNING "updateId"`.execute(tx);
    marker = row.rows[0].updateId;
    await held;
  });
  while (!marker) await pause(10);
  await pause(20);

  let nowId;
  try {
    ({ nowId } = await repo.getNow());
  } finally {
    release();
    await slowWrite;
  }
  assert.ok(nowId < marker, `cutoff ${nowId} must stay below uncommitted marker ${marker}`);

  // A write stamped up to 10 seconds in the past (clock stepped backwards) must still land above the cutoff.
  const { rows } = await sql`SELECT immich_uuid_v7(now() - interval '9 seconds') AS stepped`.execute(reader);
  const { nowId: lagged } = await repo.getNow();
  assert.ok(lagged < rows[0].stepped, `cutoff ${lagged} must stay below a marker from a 9s clock step`);

  // Poll rather than sleep exactly 10s: the host clock itself may step back during the wait.
  let after = await repo.getNow();
  for (const deadline = Date.now() + 15_000; after.nowId <= marker && Date.now() < deadline; ) {
    await pause(250);
    after = await repo.getNow();
  }
  assert.ok(after.nowId > marker, 'cutoff must advance once the writer commits and the lag passes');
  console.log('PASS: cutoff excludes open write transactions and backwards clock steps, and advances after');
}
main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(() => Promise.all([writer.destroy(), reader.destroy()]));
