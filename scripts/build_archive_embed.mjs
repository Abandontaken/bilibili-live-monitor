/** Freeze the standalone archive renderer; production needs no Node or source tree. */
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {chmodSync,copyFileSync,existsSync,mkdirSync,mkdtempSync,readFileSync,readdirSync,renameSync,rmSync,writeFileSync} from 'node:fs';
import {dirname,join,relative,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const self=fileURLToPath(import.meta.url);
const digest=data=>createHash('sha256').update(data).digest('hex');
const assets=['overview.js','overview.css','archive.html'];

export function verifyBundle(directory){
  const manifest=JSON.parse(readFileSync(join(directory,'manifest.json'),'utf8'));
  if(manifest.schema!==1||manifest.kind!=='archive_embed'||!assets.every(name=>
    manifest.files?.[name]===digest(readFileSync(join(directory,name)))))throw new Error('Archive bundle integrity mismatch');
  return manifest;
}

export function buildBundle(frontend,output){
  frontend=resolve(frontend);output=resolve(output);
  const names=['package.json','package-lock.json','archive.html'];
  function walk(directory){
    for(const entry of readdirSync(directory,{withFileTypes:true})){
      const path=join(directory,entry.name);
      if(entry.isDirectory())walk(path);
      else if(entry.isFile())names.push(relative(frontend,path));
    }
  }
  walk(join(frontend,'src'));
  const sourceFiles=Object.fromEntries(names.sort().map(name=>[name,digest(readFileSync(join(frontend,name)))]));
  const compiler=join(frontend,'node_modules/.bin/esbuild');
  const version=execFileSync(compiler,['--version'],{encoding:'utf8'}).trim();
  const identity=digest(JSON.stringify({sourceFiles,compiler:version,builder:digest(readFileSync(self))}));
  const destination=join(output,'archive-'+identity.slice(0,20));
  if(existsSync(destination)){verifyBundle(destination);return destination}
  mkdirSync(output,{recursive:true});
  const staging=mkdtempSync(join(output,'.archive-build-'));
  try{
    execFileSync(compiler,[join(frontend,'src/ArchiveEmbed.tsx'),'--bundle','--minify','--format=iife',
      '--define:process.env.NODE_ENV="production"','--outfile='+join(staging,'overview.js')],{stdio:'pipe'});
    copyFileSync(join(frontend,'archive.html'),join(staging,'archive.html'));
    const files=Object.fromEntries(assets.map(name=>[name,digest(readFileSync(join(staging,name)))]));
    for(const name of names){
      const target=join(staging,'source/frontend',name);mkdirSync(dirname(target),{recursive:true});
      copyFileSync(join(frontend,name),target);chmodSync(target,0o444);
      if(digest(readFileSync(target))!==sourceFiles[name])throw new Error('Archive input changed during build');
    }
    copyFileSync(self,join(staging,'source/build_archive_embed.mjs'));
    writeFileSync(join(staging,'manifest.json'),JSON.stringify({schema:1,kind:'archive_embed',
      source_identity:identity,compiler_version:version,files,source_files:sourceFiles},null,2)+'\n');
    for(const name of [...assets,'manifest.json','source/build_archive_embed.mjs'])chmodSync(join(staging,name),0o444);
    verifyBundle(staging);renameSync(staging,destination);
  }finally{if(existsSync(staging))rmSync(staging,{recursive:true})}
  return destination;
}

if(process.argv[1]&&resolve(process.argv[1])===self){
  const args=process.argv.slice(2);
  const option=(name,fallback)=>args.includes(name)?args[args.indexOf(name)+1]:fallback;
  console.log(buildBundle(option('--frontend',join(dirname(self),'../frontend')),
    option('--output',join(dirname(self),'../.local/archive-assets'))));
}
