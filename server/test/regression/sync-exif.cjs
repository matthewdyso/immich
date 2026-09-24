// Run in the built server image against a DISPOSABLE, empty PostgreSQL database.
const assert = require('node:assert/strict');
const { Kysely, sql } = require('kysely');
const { getKyselyConfig } = require('./dist/utils/database.js');
const { columns } = require('./dist/database.js');
const { SyncRepository } = require('./dist/repositories/sync.repository.js');

const marker = (n) => `00000000-0000-0000-0000-${String(n).padStart(12, '0')}`;
const userId = marker(1000);
const db = new Kysely(getKyselyConfig({ connectionType: 'url', url: process.env.TEST_DATABASE_URL }));
const collect = async (stream) => { const rows = []; for await (const row of stream) rows.push(row); return rows; };

async function main() {
  // This database belongs only to the disposable regression container.
  await sql`CREATE TABLE asset (id uuid PRIMARY KEY, "ownerId" uuid NOT NULL, "updateId" uuid NOT NULL)`.execute(db);
  const fields = columns.syncAssetExif.map((name) => name.split('.')[1]);
  const definitions = fields.map((name) => `"${name}" ${name === 'assetId' ? 'uuid PRIMARY KEY REFERENCES asset(id)' : 'text'}`);
  await sql.raw(`CREATE TABLE asset_exif (${definitions.join(',')}, "updateId" uuid NOT NULL)`).execute(db);
  const repo = new SyncRepository(db);
  const insert = async (id, assetMarker, exifMarker, owner = userId) => {
    await db.insertInto('asset').values({ id: marker(id), ownerId: owner, updateId: marker(assetMarker) }).execute();
    await db.insertInto('asset_exif').values({ assetId: marker(id), city: 'Chicago', updateId: marker(exifMarker) }).execute();
  };
  // The Live Photo video was modified after its metadata and the sync cutoff.
  await insert(1, 30, 10);
  // This later EXIF record advances the checkpoint while the video is deferred.
  await insert(2, 5, 15);
  await insert(3, 20, 7); // Boundary: parent updated exactly at the cutoff.
  await insert(4, 4, 6, marker(1001)); // A different user's private asset.
  const first = await collect(repo.assetExif.getUpserts({ userId, nowId: marker(20) }));
  assert.deepEqual(first.map((r) => r.assetId), [marker(2)], 'must not send metadata for a parent excluded by the cutoff');
  const ack = { type: 'AssetExifV1', updateId: first.at(-1).updateId };
  const second = await collect(repo.assetExif.getUpserts({ userId, nowId: marker(40), ack }));
  assert.deepEqual(second.map((r) => r.assetId), [marker(3), marker(1)], 'deferred EXIF must not be skipped after another record advances the cursor');
  assert.deepEqual(second.map((r) => r.updateId), [marker(20), marker(30)]);
  const third = await collect(repo.assetExif.getUpserts({ userId, nowId: marker(50), ack: { ...ack, updateId: marker(30) } }));
  assert.equal(third.length, 0, 'acknowledged records must not repeat');
  await db.updateTable('asset_exif').set({ updateId: marker(45), city: 'Tucson Estates' }).where('assetId', '=', marker(1)).execute();
  const fourth = await collect(repo.assetExif.getUpserts({ userId, nowId: marker(50), ack: { ...ack, updateId: marker(30) } }));
  assert.equal(fourth.length, 1);
  assert.equal(fourth[0].city, 'Tucson Estates');
  assert.equal(fourth[0].updateId, marker(45));
  console.log('PASS: parent cutoff, deferred retry, ordering, user isolation, cursor advancement, later EXIF update');
}
main().catch((error) => { console.error(error); process.exitCode = 1; }).finally(() => db.destroy());
