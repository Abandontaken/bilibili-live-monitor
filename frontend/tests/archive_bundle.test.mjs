import {test} from 'node:test';
import assert from 'node:assert/strict';
import {chmodSync,mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {buildBundle,verifyBundle} from '../../scripts/build_archive_embed.mjs';

test('archive renderer is reproducible and ships standalone JS, CSS, template and source provenance',()=>{
  const directory=mkdtempSync(join(tmpdir(),'archive-bundle-'));
  try{
    const frontend=resolve(dirname(fileURLToPath(import.meta.url)),'..');
    const first=buildBundle(frontend,join(directory,'first'));
    const second=buildBundle(frontend,join(directory,'second'));
    const a=verifyBundle(first),b=verifyBundle(second);
    assert.equal(a.source_identity,b.source_identity);
    assert.deepEqual(a.files,b.files);
    assert.ok(a.source_files['src/ArchiveEmbed.tsx']);
    assert.match(readFileSync(join(first,'archive.html'),'utf8'),/__DATA__/);
    assert.ok(readFileSync(join(first,'overview.js')).length>1000);
    assert.ok(readFileSync(join(first,'overview.css')).length>100);
    chmodSync(join(first,'overview.js'),0o644);
    writeFileSync(join(first,'overview.js'),'tampered');
    assert.throws(()=>verifyBundle(first),/integrity/);
    assert.throws(()=>buildBundle(frontend,join(directory,'first')),/integrity/);
  }finally{rmSync(directory,{recursive:true,force:true})}
});
