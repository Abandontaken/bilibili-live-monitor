import React, {useEffect, useState} from 'react';

type Resources={source:'yamon';scope:'system';epoch:number;cpu_percent:number;
  memory_used_gib:number;memory_total_gib:number;gpu_percent:number|null;
  download_mbps:number|null;upload_mbps:number|null};

function validResources(value:unknown):value is Resources{
  if(!value||typeof value!=='object')return false;
  const row=value as Record<string,unknown>;
  const number=(key:string)=>typeof row[key]==='number'&&Number.isFinite(row[key])&&(row[key] as number)>=0;
  return row.source==='yamon'&&row.scope==='system'
    &&['epoch','cpu_percent','memory_used_gib','memory_total_gib'].every(number)
    &&(row.cpu_percent as number)<=100&&(row.memory_total_gib as number)>0
    &&(row.memory_used_gib as number)<=(row.memory_total_gib as number)
    &&['gpu_percent','download_mbps','upload_mbps'].every(k=>row[k]===null||number(k))
    &&(row.gpu_percent===null||(row.gpu_percent as number)<=100);
}

export function SystemResources(){
  const [data,setData]=useState<Resources|null>(null);
  const [now,setNow]=useState(Date.now());
  useEffect(()=>{
    let alive=true,timer:ReturnType<typeof setTimeout>,controller:AbortController;
    async function refresh(){
      controller=new AbortController();
      const deadline=setTimeout(()=>controller.abort(),4000);
      try{
        const response=await fetch(import.meta.env.BASE_URL+'api/dashboard/resources',
          {cache:'no-store',signal:controller.signal});
        if(!response.ok)throw new Error('unavailable');
        const next:unknown=await response.json();
        if(alive)setData(validResources(next)?next:null);
      }catch{if(alive)setData(null)}
      finally{clearTimeout(deadline);if(alive){setNow(Date.now());timer=setTimeout(()=>void refresh(),5000)}}
    }
    void refresh();return()=>{alive=false;controller?.abort();clearTimeout(timer)};
  },[]);
  const stale=!!data&&(now-data.epoch*1000>30000||data.epoch*1000-now>5000);
  const format=(value:number|null|undefined,digits=1)=>!data||stale||value==null?'—':value.toFixed(digits);
  return <section aria-label="整机资源监控（YAMON）">
    <div className="metrics">
      <div><span>整机 CPU · 全机总算力 100%</span><strong>{format(data?.cpu_percent)}<small> %</small></strong></div>
      <div><span>整机 GPU</span><strong>{format(data?.gpu_percent,0)}<small> %</small></strong></div>
      <div><span>整机内存 · 已用 / 总量</span><strong>{format(data?.memory_used_gib)} / {format(data?.memory_total_gib,0)}<small> GiB</small></strong></div>
      <div><span>整机网络 · 下载 / 上传</span><strong>{format(data?.download_mbps)} / {format(data?.upload_mbps)}<small> Mbps</small></strong></div>
    </div>
    {!data?<p role="status">YAMON 采样暂不可用</p>:stale?<p role="status">采样数据滞后</p>:null}
    <p className="footnote">数据源自 YAMON 整机采样，包含其他应用开销，不代表本监控进程单独占用。</p>
  </section>;
}
