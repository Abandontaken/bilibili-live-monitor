import React, {useCallback, useEffect, useLayoutEffect, useRef, useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';
import './homepage.css';
import {giftLabel, giftText, type Gift} from './gifts';
import {transcriptLabel} from './transcripts';
import {Assistant} from './Assistant';
import {AutomationControls,AutomationProvider} from './Automation';
import {RoomIssue} from './RoomIssue';
import {InstanceProvider, useApi, useInstance, type Instance} from './Instance';
import {RoomArchive} from './RoomArchive';
import {RoomLinks} from './RoomLinks';
import {RoomTrend} from './RoomTrend';
import {RoomPeriodMetrics} from './RoomPeriodMetrics';
import {SystemResources} from './SystemResources';

type Segment = {seq:number; start_utc:string; end_utc:string; text:string; status:string;
  queue_wait_ms:number; infer_ms:number; rms_dbfs:number; error:string|null; coverage_ratio?:number|null};
type LikeSummary = {reported_total?:number|null;observed_utc?:string|null;notifications:number;identified_users:number};
type Room = {room_id:string; title:string; url:string; cover_url:string; status:string;
   stale?:boolean;last_success?:string|null;
   display_name?:string;fixed_task?:boolean;current_issue?:boolean;policy_paused?:boolean;
  instance_label?:string;account_mode?:string;result_url?:string;workflow_stage?:string;home_url?:string;
  like_summary?:LikeSummary|null;
  reconnect_attempts?:number;next_retry_utc?:string|null;
  audio_error?:string|null;
  capture_health?:{state:string;last_hour:{page_reloads:number;local_restarts:number;invalid_seconds:number;gap_seconds:number}}|null;
  record_mode:'none'|'audio'|'video';asr_enabled:boolean;preview_enabled:boolean;
  recording_status:string;recorded_seconds:number;recording_files:number;recording_bytes:number;
   error:string|null; updated_at?:string; chunk_seconds:number; segments:Segment[];items:Item[]};
type Item = {id:number;room_id:string;kind:'transcript'|'danmaku'|'gift'|'entry'|'like';at_utc:string;received_utc:string;text:string;
  entry?:{event_type:string;time_source:'event'|'received'}|null;
  transcript_version?:'realtime'|'offline'|null;stable_text?:string|null;is_final?:boolean|null;
  stream_id?:string|null;
  sender_uid?:string|null;guard_level?:number|null;
  author?:string|null;seq?:number|null;status?:string|null;error?:string|null;
  gift?:Gift|null};
type Change = {kind:'room';data:Room}|{kind:'segment';data:Segment&{room_id:string}}|{kind:'item';data:Item}
  |{kind:'likes';data:{room_id:string;summary:LikeSummary}};
type State = {rooms:Room[]; queue_depth:number; queue_capacity:number; asr_ready:boolean;
   fixed_task_count?:number;raw_fixed_count?:number;
  asr_enabled:boolean;
  overload_count:number; inflight:{room_id:string;seq:number}|null;active_count:number;pending_count:number;
  asr_restarts:number;asr_retries:number;asr_error:string|null};
const labels:Record<string,string> = {
  starting:'启动中',capturing:'采集中',queued:'排队中',transcribing:'转写中',stopped:'已停止',
  error:'异常',silent:'静音跳过',done:'已转写',empty:'未识别到文字',cancelled:'已取消',truncated:'识别截断',reconnecting:'正在重连',waiting:'守候中'
};
const time = (iso:string) => new Date(iso).toLocaleTimeString('zh-CN',{hour12:false});

type Resources = {epoch:number;sample_seconds:number;cpu_cores:number;rss_gib:number;
  system_gpu_percent:number|null;external_download_mbps:number;external_upload_mbps:number;
  chrome_download_mbps:number;ffmpeg_download_mbps:number;recording_write_mbps:number};

function validResources(value:unknown):value is Resources{
  if(!value||typeof value!=='object')return false;
  const row=value as Record<string,unknown>;
  const keys=['epoch','sample_seconds','cpu_cores','rss_gib','external_download_mbps',
    'external_upload_mbps','chrome_download_mbps','ffmpeg_download_mbps','recording_write_mbps'];
  return keys.every(k=>typeof row[k]==='number'&&Number.isFinite(row[k])&&(row[k] as number)>=0)
    &&(row.system_gpu_percent===null||(typeof row.system_gpu_percent==='number'
      &&Number.isFinite(row.system_gpu_percent)&&row.system_gpu_percent>=0&&row.system_gpu_percent<=100));
}

function ResourceMetrics(){
  const {base}=useInstance();
  const [data,setData]=useState<Resources|null>(null);
  const [now,setNow]=useState(Date.now());
  useEffect(()=>{
    const controller=new AbortController();let alive=true;
    async function refresh(){
      setNow(Date.now());
      try{const response=await fetch(base+'assets/resource_metrics.json',{cache:'no-store',signal:controller.signal});
        if(response.ok){const next:unknown=await response.json();if(alive)setData(validResources(next)?next:null)}else if(alive)setData(null)}catch{if(alive)setData(null)}
    }
    void refresh();const timer=setInterval(()=>void refresh(),5000);
    return()=>{alive=false;controller.abort();clearInterval(timer)};
  },[]);
   if(!data)return <section className="metrics resource-unavailable"><div><span>监控 CPU</span><strong>—</strong></div><div><span>监控 RSS</span><strong>—</strong></div><p role="status">资源暂不可用</p></section>;
  const stale=now-data.epoch*1000>30000;
  return <section aria-label="机器资源与带宽">
    <div className="metrics">
      <div><span>监控 CPU · 1 核 = 100%</span><strong>{stale?'—':data.cpu_cores.toFixed(1)}<small> 核</small></strong></div>
      <div><span>整机 GPU · 含其他应用</span><strong>{stale||data.system_gpu_percent==null?'—':data.system_gpu_percent.toFixed(0)}<small> %</small></strong></div>
      <div><span>监控 RSS 合计 · 含共享页重复</span><strong>{stale?'—':data.rss_gib.toFixed(1)}<small> GiB</small></strong></div>
      <div><span>监控外网下载 · 约 {data.sample_seconds.toFixed(0)} 秒均值</span><strong>{stale?'—':data.external_download_mbps.toFixed(1)}<small> Mbps</small></strong></div>
    </div>
    <p className="footnote">{stale?'资源采样已过期，等待采样器恢复。':`Chrome 下载 ${data.chrome_download_mbps.toFixed(1)} Mbps · ffmpeg 下载 ${data.ffmpeg_download_mbps.toFixed(1)} Mbps · 上传 ${data.external_upload_mbps.toFixed(1)} Mbps · 录制写入 ${(data.recording_write_mbps/8).toFixed(1)} MB/s。`} 网络值不含本机回环；连接切换可能低估。GPU 为整机即时值，不能全部归因于监控。</p>
  </section>;
}

type WatchCandidate = {url:string;room_id?:string;title:string;blocked:boolean;blocked_alias?:boolean;watch_state?:string;
  status_checked_utc?:string;status_error?:string;last_error?:string;guard_error?:string;
  guard_snapshot?:{reported_total:number;complete:boolean;observed_utc:string}};
type WatchSummary = {policy:{mode:string;enabled:boolean;fixed_rooms?:string[]};status_refused?:string;
  candidates:WatchCandidate[];sources:{room_id:string;status:string;recording_status:string}[];
  message_health:Record<string,{state?:string;last_message_utc?:string}>};

export function WatchStatus(){
  const api=useApi();
  const [summary,setSummary]=useState<WatchSummary|null>(null);
  const [failed,setFailed]=useState(false);
  useEffect(()=>{let alive=true;let timer:ReturnType<typeof setTimeout>;
    async function refresh(){try{const data=await api('platform');if(alive){setSummary(data);setFailed(false)}}
      catch{if(alive)setFailed(true)}finally{if(alive)timer=setTimeout(()=>void refresh(),5000)}}
    void refresh();return()=>{alive=false;clearTimeout(timer)}},[]);
  if(!summary)return failed?<p role="alert">守候状态读取失败，尚未取得有效结果。</p>:null;
  if(summary.policy.mode!=='fixed')return null;
  const names:Record<string,string>={waiting:'等待开播',live:'已开播，等待录制',unknown:'状态未知',
    retrying:'录制恢复中',attention:'需要人工处理',recording_pending:'采集已启动',confirming_offline:'正在确认下播'};
  return <section aria-label="固定房间守候"><h2>固定房间守候{!summary.policy.enabled?' · 已暂停':''}</h2>
    {failed&&<p role="alert">守候状态刷新失败，以下为上次结果。</p>}
    {summary.status_refused&&<p role="alert">状态接口暂停：{summary.status_refused}</p>}
    {summary.candidates.filter(c=>!summary.policy.fixed_rooms?.length||summary.policy.fixed_rooms.includes(c.url)).map(c=>{const rid=c.room_id||c.url.split('/').at(-1)||'';
      const source=summary.sources.find(r=>r.room_id===rid);const chat=summary.message_health[rid];
      return <article key={c.url}><p><a href={c.url} target="_blank" rel="noreferrer">{c.title||rid}</a> · {
        c.blocked||c.blocked_alias?'需要人工处理':!summary.policy.enabled?'已暂停':
        source?.status==='capturing'&&source.recording_status==='recording'?'正在录制':
        names[c.watch_state||'unknown']||'状态未知'} · 最近查播 {c.status_checked_utc?time(c.status_checked_utc):'尚未查询'}</p>
        <p>弹幕连接：{chat?.state==='connected'?'已连接':chat?.state==='disconnected'?'已断开':chat?.state==='error'?'连接异常':'未确认'} · 最近消息 {chat?.last_message_utc?time(chat.last_message_utc):'尚无'} · 舰队快照：{c.guard_snapshot?`${c.guard_snapshot.reported_total} 人（${c.guard_snapshot.complete?'本次完整':'部分数据'}）`:'尚无'}</p>
        {(c.status_error||c.last_error||c.guard_error)&&<p className="room-error">{c.status_error||c.last_error||c.guard_error}</p>}
      </article>})}</section>;
}

const RoomCard = React.memo(function RoomCard({room,status,onError}:{room:Room;status:string;onError:(s:string)=>void}){
  const api=useApi();const {base}=useInstance();
  const scroller=useRef<HTMLDivElement>(null);
  const [following,setFollowing]=useState(true);
  const latest=room.segments.at(-1);
  useLayoutEffect(()=>{
    if(following&&scroller.current)scroller.current.scrollTop=scroller.current.scrollHeight;
  },[room.items,following]);
  return <article className="card">
    <div className="card-body"><div className="room-heading"><a href={room.url} target="_blank" rel="noreferrer">{room.display_name||room.title.replace(/ - 哔哩哔哩直播.*$/,'')}</a><span className={'badge '+status}>{room.stale?'状态未知':labels[status]||status}</span></div>
    {room.stale&&<p role="status">实例暂不可用 · 以下为上次结果 · {room.last_success?new Date(room.last_success).toLocaleString('zh-CN'):'尚未取得快照'}</p>}
    <RoomLinks liveUrl={room.url} archiveUrl={room.home_url?base+room.home_url:undefined}/>
    <div className="room-sub"><span>房间 {room.room_id}{room.instance_label?` · ${room.instance_label}`:''}</span><button className="text-button" disabled={room.status==='stopped'||room.status==='waiting'} onClick={()=>api(`rooms/${room.room_id}/stop`,{method:'POST'}).catch(e=>onError(e.message))}>停止监控</button></div>
    {room.result_url&&<p className="room-sub"><a href={base+room.result_url} target="_blank" rel="noreferrer">{resultLabel(room)}</a></p>}
    {!room.stale&&<RoomIssue status={room.status} error={room.error} updatedAt={room.updated_at}/>}
    {!room.stale&&room.audio_error&&room.status==='capturing'&&<p className="error">音频质量或媒体进度异常：{room.audio_error}</p>}
    {room.capture_health&&<p className="room-sub">近一小时（当前采集任务）：整页重载 {room.capture_health.last_hour.page_reloads} 次 · 采音重建 {room.capture_health.last_hour.local_restarts} 次 · 异常音频 {Math.round(room.capture_health.last_hour.invalid_seconds)} 秒 · 采集缺口 {Math.round(room.capture_health.last_hour.gap_seconds)} 秒</p>}
    {!!room.reconnect_attempts&&<p className="room-sub">连续失败 {room.reconnect_attempts} 次{room.next_retry_utc?` · 下次重试 ${time(room.next_retry_utc)}`:''}</p>}
    {room.record_mode!=='none'&&<p className="room-sub">录制 {room.recording_status==='recording'?'进行中':room.recording_status==='starting'?'启动中':room.recording_status==='stopped'?'已结束':room.recording_status} · {(room.recorded_seconds/60).toFixed(1)} 分钟 · {(room.recording_bytes/1e6).toFixed(1)} MB · {room.recording_files} 文件</p>}
    {room.home_url&&<RoomPeriodMetrics base={base} roomId={room.room_id}/>}
    <AutomationControls rooms={[room]} roomId={room.room_id}/>
    {room.home_url&&<RoomTrend base={base} roomId={room.room_id}/>}
    <div className="timeline-tools"><button className="text-button" aria-pressed={following} onClick={()=>{setFollowing(true);if(scroller.current)scroller.current.scrollTop=scroller.current.scrollHeight}}>{following?'跟随最新':'已暂停·回最新'}</button><a href={base+'api/export?room_id='+room.room_id}>导出本房快照</a></div>
    <div className={'segments'+(following?' following':'')} ref={scroller} onScroll={e=>{const n=e.currentTarget;setFollowing(n.scrollHeight-n.scrollTop-n.clientHeight<32)}}>
      {!room.items.length?<p className="waiting">等待弹幕、礼物或首个音频切片…</p>:room.items.map(item=><div className={'segment '+item.kind+' '+(item.status||'')} key={item.id}>
        <div className="segment-meta"><time>{time(item.at_utc)}</time><span>{item.kind==='like'?'点赞通知 · 接收时间':item.kind==='entry'?'入场通知'+(item.entry?.time_source==='received'?' · 接收时间':''):item.kind==='gift'?giftLabel(item.gift)+(item.gift?.time_source==='received'?' · 接收时间':''):item.kind==='danmaku'?'弹幕':item.transcript_version==='realtime'?transcriptLabel(item):`语音转写 #${item.seq} · ${labels[item.status||'']||item.status}`}</span>{(item.kind==='entry'||item.kind==='like')&&<span>{item.sender_uid?`UID ${item.sender_uid}`:'身份未确认'}</span>}</div>
        <p>{item.author&&<b className="chat-author" title={item.sender_uid?`UID ${item.sender_uid}`:'身份未确认'}>{item.author}{item.guard_level?` · ${{1:'总督',2:'提督',3:'舰长'}[item.guard_level]||'大航海'}`:''}：</b>}{(item.kind==='gift'?giftText(item.gift,item.text):item.text)||item.error||(item.status==='silent'?'音量低于阈值，已跳过识别':item.status==='empty'?'本段未识别到文字':'等待识别…')}</p>
        {item.error&&item.text&&<p className="room-error">{item.error}</p>}
      </div>)}
    </div>
    <details className="technical-details"><summary>诊断信息</summary><footer>{room.asr_enabled?<><span>等待 {latest?`${(latest.queue_wait_ms/1000).toFixed(1)}s`:'—'}</span><span>推理 {latest?`${(latest.infer_ms/1000).toFixed(1)}s`:'—'}</span><span>{latest?`${latest.rms_dbfs} dBFS`:'等待音频'}</span><span>覆盖 {latest?.coverage_ratio!=null?`${(latest.coverage_ratio*100).toFixed(0)}%`:'—'}</span></>:<span>ASR 禁用</span>}</footer></details>
    </div></article>;
});

type Report=(id:string,state:State,connected:boolean)=>void;
function SnapshotApp({onState}:{onState?:Report}){
  const {base,label,id,mode}=useInstance();const api=useApi();
  const [state,setState] = useState<State>({rooms:[],queue_depth:0,queue_capacity:20,asr_ready:false,asr_enabled:false,overload_count:0,inflight:null,active_count:0,pending_count:0,asr_restarts:0,asr_retries:0,asr_error:null});
  const [recordMode,setRecordMode]=useState('audio');
  const [asrEnabled,setAsrEnabled]=useState(false);
  const [connected,setConnected] = useState(false);
  const [lastSnapshot,setLastSnapshot]=useState<string|null>(null);
  const [url,setUrl] = useState('');
  const seconds = 5;
  const [busy,setBusy] = useState(false);
  const [error,setError] = useState('');
  const [candidates,setCandidates] = useState<{url:string;title:string}[]>([]);
  const [fullscreen,setFullscreen]=useState(false);
  const timer = useRef<ReturnType<typeof setTimeout>|null>(null);
  const changes=useRef<Change[]>([]);
  useEffect(()=>{onState?.(id,state,connected)},[id,state,connected,onState]);
  useEffect(()=>{const update=()=>setFullscreen(Boolean(document.fullscreenElement));
    document.addEventListener('fullscreenchange',update);return()=>document.removeEventListener('fullscreenchange',update)},[]);
  useEffect(()=>{
    let alive=true;
    const enqueue=(change:Change)=>{changes.current.push(change);if(timer.current)return;
      timer.current=setTimeout(()=>{timer.current=null;const batch=changes.current;changes.current=[];
        if(!alive)return;
        setState(s=>{let rooms=s.rooms;
          for(const change of batch){const id=change.data.room_id;const index=rooms.findIndex(r=>r.room_id===id);
            if(change.kind==='room'){
              const old=rooms[index];const next={...change.data,like_summary:change.data.like_summary??old?.like_summary,segments:old?.segments||[],items:old?.items||[]};
              rooms=index<0?[...rooms,next]:rooms.map((r,i)=>i===index?next:r);
            }else if(index>=0){const old=rooms[index];let next:Room;
              if(change.kind==='likes'){next={...old,like_summary:change.data.summary}}
              else if(change.kind==='segment'){const seg=change.data;next={...old,segments:[...old.segments.filter(x=>x.seq!==seg.seq),seg].sort((a,b)=>a.seq-b.seq).slice(-20)}}
              else{const item=change.data;next={...old,items:[...old.items.filter(x=>x.id!==item.id),item].sort((a,b)=>Date.parse(b.received_utc)-Date.parse(a.received_utc)||b.id-a.id).slice(0,80).sort((a,b)=>Date.parse(a.at_utc)-Date.parse(b.at_utc)||a.id-b.id)}}
              rooms=rooms.map((r,i)=>i===index?next:r);
            }
          }return {...s,rooms}});
      },200)};
    const events = new EventSource(base + 'api/events');
    events.onopen=()=>{};
    events.onerror=()=>setConnected(false);
    events.addEventListener('snapshot',e=>{changes.current=[];setLastSnapshot(new Date().toISOString());setConnected(true);setState(JSON.parse((e as MessageEvent).data))});
    events.addEventListener('unavailable',()=>setConnected(false));
    for(const kind of ['room','segment','item','likes'] as const)events.addEventListener(kind,e=>enqueue({kind,data:JSON.parse((e as MessageEvent).data)}));
    events.addEventListener('metrics',e=>setState(s=>({...s,...JSON.parse((e as MessageEvent).data)})));
    return ()=>{alive=false;events.close();if(timer.current)clearTimeout(timer.current);timer.current=null;changes.current=[]};
  },[]);
  async function add(value:string){
    setBusy(true);setError('');
    try{await api('rooms',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:value,chunk_seconds:seconds,record_mode:recordMode,asr_enabled:asrEnabled,preview_enabled:false})});setUrl('')}
    catch(e){setError((e as Error).message)}finally{setBusy(false)}
  }
  const active=state.active_count+state.pending_count;
  return <main>
    {!onState&&<><header><div><div className="eyebrow">本地 CDP 静音捕获 · Qwen3-ASR</div><h1>B站多路直播监控看板{label?` · ${label}`:''}</h1><p>多路直播语音、弹幕与礼物混合时间线。</p></div>
      <div className="header-actions"><button className="secondary" onClick={async()=>{try{if(document.fullscreenElement)await document.exitFullscreen();else await document.documentElement.requestFullscreen()}catch(e){setError((e as Error).message)}}}>{fullscreen?'退出全屏':'进入全屏'}</button><span className={'connection '+(connected?'online':'')}><i/>{connected?'实时连接正常':'正在重连'}</span></div></header>
    <details open className="dashboard-details"><summary>系统状态与资源</summary><section className="metrics">
      <div><span>监控任务（含重连 / 启动）</span><strong>{active}<small> 路</small></strong></div>
      <div><span>队列深度</span><strong>{state.queue_depth}<small> / {state.queue_capacity}</small></strong></div>
      <div><span>ASR · 重启 {state.asr_restarts} / 重试 {state.asr_retries}</span><strong className="word">{!state.asr_enabled?'禁用':state.asr_ready?'已就绪':state.asr_error?'等待恢复':'正在加载'}</strong></div>
      <div><span>队列过载次数</span><strong className={state.overload_count?'warning':''}>{state.overload_count}</strong></div>
    </section>
    <ResourceMetrics/></details></>}
    <div className="section-title"><h2>{label||'直播间'} <span>{state.fixed_task_count??state.rooms.length}</span></h2><span className={'connection '+(connected?'online':'')}>{connected?'实时连接正常':'正在重连'}</span></div>
    {mode!=='anonymous'&&<AutomationControls settingsOnly/>}
    <details className="dashboard-details"><summary>添加直播间</summary><form onSubmit={e=>{e.preventDefault();void add(url)}}>
      <input aria-label="直播间链接或房间号" placeholder="输入直播间链接或房间号" value={url} onChange={e=>setUrl(e.target.value)} required/>
      <select aria-label="保存类型" value={recordMode} onChange={e=>setRecordMode(e.target.value)}><option value="audio">只保存音频</option><option value="video">保存视频＋音频</option><option value="none">不保存媒体</option></select>
      <label><input type="checkbox" checked={asrEnabled} onChange={e=>setAsrEnabled(e.target.checked)}/>ASR</label>
      <button disabled={busy}>{busy?'正在连接…':'添加直播间'}</button>
      <button type="button" className="secondary" disabled={busy} onClick={async()=>{
        setBusy(true);setError('');try{setCandidates(await api('discover?limit=20'))}catch(e){setError((e as Error).message)}finally{setBusy(false)}
      }}>发现直播</button>
    </form></details>
    {error&&<div className="alert" role="alert">{error}<button onClick={()=>setError('')}>×</button></div>}
    {state.asr_error&&<div className="alert" role="alert">模型：{state.asr_error}</div>}
    {candidates.length>0&&<section className="candidates">{candidates.map(c=><button disabled={busy} key={c.url} className="secondary" onClick={()=>void add(c.url)}>{c.title||c.url}</button>)}</section>}
    <a className="export-link" href={base+'api/export'}>导出全部快照</a>
    {state.rooms.length===0&&<section className="empty">当前暂无监控房间，请输入直播间链接或房间号开始监听。</section>}
    <section className={'grid '+(mode==='anonymous'?'anonymous-grid':'account-grid')}>{state.rooms.filter(r=>r.fixed_task!==false).map(original=>{const room={...original,stale:!connected,last_success:lastSnapshot};return <div id={'room-'+base.replace(/\W/g,'-')+room.room_id} key={room.room_id}>{mode==='anonymous'?<CompactRoom room={room}/>:<RoomCard room={room} status={room.status==='capturing'&&state.inflight?.room_id===room.room_id?'transcribing':room.status} onError={setError}/>}</div>})}</section>
    {state.rooms.some(r=>r.fixed_task===false)&&<details className="dashboard-details"><summary>临时任务 · {state.rooms.filter(r=>r.fixed_task===false).length}</summary><div className="grid">{state.rooms.filter(r=>r.fixed_task===false).map(room=>mode==='anonymous'?<CompactRoom key={room.room_id} room={{...room,stale:!connected,last_success:lastSnapshot}}/>:<RoomCard key={room.room_id} room={{...room,stale:!connected,last_success:lastSnapshot}} status={room.status} onError={setError}/>)}</div></details>}
    <details className="technical-details"><summary>数据说明</summary><p className="footnote">每房间显示最近80条消息。完整时间线保存在本地，可导出快照。历史统计与金额口径见按日汇总。</p></details>
  </main>;
}
function resultLabel(room:Room){
  return room.workflow_stage==='live'?'查看本场直播结果':room.workflow_stage==='completed'||room.workflow_stage==='completed_talk'?'查看最近已结束场次结果':'查看最近场次结果（处理中）';
}
function CompactRoom({room}:{room:Room}){
 const {base}=useInstance();
 return <article className="card compact-room"><div className="card-body">
  <div className="room-heading"><strong title={room.display_name||room.title}>{room.display_name||room.title}</strong><span className={'badge '+room.status}>{room.stale?'状态未知':room.policy_paused?'已暂停':labels[room.status]||room.status}</span></div>
  {room.stale&&<p role="status">实例暂不可用 · 以下为上次结果 · {room.last_success?new Date(room.last_success).toLocaleString('zh-CN'):'尚未取得快照'}</p>}
  <small>房间 {room.room_id}</small>
  {!room.stale&&room.current_issue&&<RoomIssue status="error" error={room.error||room.audio_error||'需要人工处理'}/>}
  {room.home_url?<RoomPeriodMetrics base={base} roomId={room.room_id}/>:<p>归档暂时无法读取</p>}
  <RoomLinks liveUrl={room.url} archiveUrl={room.home_url?base+room.home_url:undefined}/>
   {room.result_url&&<a href={base+room.result_url}>{resultLabel(room)}</a>}
 </div></article>;
}
function SingleApp({onState}:{onState?:Report}){
  const instance=useInstance();
  const [page,setPage]=useState<'snapshot'|'assistant'>('snapshot');
   if(instance.mode==='anonymous')return <SnapshotApp onState={onState}/>;
  return <><nav className="top-nav" aria-label="页面">
    <button aria-pressed={page==='snapshot'} onClick={()=>setPage('snapshot')}>快照</button>
    <button aria-pressed={page==='assistant'} onClick={()=>setPage('assistant')}>直播助手</button>
   </nav><div hidden={page!=='snapshot'}><SnapshotApp onState={onState}/></div>{page==='assistant'&&<main><Assistant/></main>}</>;
}
function SystemHealth({instances,reports}:{instances:Instance[];reports:Record<string,{state:State;connected:boolean}>}){
 const rows=instances.map(i=>reports[i.id]),complete=rows.every(r=>r?.connected&&r.state.fixed_task_count!=null);
 const known=rows.filter((r):r is {state:State;connected:boolean}=>!!r?.connected&&r.state.fixed_task_count!=null);
 const sum=(key:'fixed_task_count'|'queue_depth'|'queue_capacity')=>known.reduce((n,r)=>n+(r.state[key]??0),0);
 const rooms=known.flatMap(r=>r.state.rooms.filter(r=>r.fixed_task));
 const value=(n:number)=>complete?n:known.length?`${n} + —`:'—';
 return <section className="system-health" aria-label="系统状态与资源"><header><h1>B站多路直播监控看板</h1><button className="secondary" onClick={()=>{void (document.fullscreenElement?document.exitFullscreen():document.documentElement.requestFullscreen()).catch(()=>{})}}>进入全屏</button></header>
 <p role="status">{complete?'实时连接正常':'部分实例暂不可用'}</p><div className="metrics">
 <div><span>配置监控任务</span><strong>{value(sum('fixed_task_count'))}<small> 路</small></strong></div>
 <div><span>当前实际采集</span><strong>{value(rooms.filter(r=>r.status==='capturing').length)}</strong><small>录制 {value(rooms.filter(r=>r.status==='capturing'&&r.recording_status==='recording').length)}</small></div>
 <div><span>当前异常</span><strong>{value(rooms.filter(r=>r.current_issue).length)}</strong></div>
 <div><span>ASR 队列</span><strong>{value(sum('queue_depth'))}<small> / {value(sum('queue_capacity'))}</small></strong><small>处理中 {value(known.filter(r=>r.state.inflight).length)}</small></div>
 </div><details><summary>诊断信息</summary>{instances.map(i=>{const r=reports[i.id];return <p key={i.id}>{i.label} · {r?.connected?'实时连接正常':'暂不可用'} · 配置 {r?.state.fixed_task_count??'—'} / 原始 {r?.state.raw_fixed_count??'—'} · ASR {r?.connected?(!r.state.asr_enabled?'禁用':r.state.asr_error?'等待恢复':r.state.asr_ready?'已就绪':'正在加载'):'—'}</p>})}</details>
  <SystemResources/></section>;
}
export function App(){
  const [instances,setInstances]=useState<Instance[]|null>(null);
  const [reports,setReports]=useState<Record<string,{state:State;connected:boolean}>>({});
  const report=useCallback<Report>((id,state,connected)=>setReports(old=>({...old,[id]:{state,connected}})),[]);
  useEffect(()=>{let alive=true;fetch(import.meta.env.BASE_URL+'api/dashboard/instances')
    .then(r=>{if(!r.ok)throw new Error('single');return r.json()})
    .then((rows:Instance[])=>{if(alive)setInstances(rows.map(row=>({...row,base:import.meta.env.BASE_URL+row.base})))})
    .catch(()=>{if(alive)setInstances([])});return()=>{alive=false}},[]);
  if(instances===null)return <main>正在连接监控实例…</main>;
   if(!instances.length)return <AutomationProvider><SingleApp/></AutomationProvider>;
   return <div className="dashboard-shell"><SystemHealth instances={instances} reports={reports}/>{instances.map(instance=><section key={instance.id} aria-label={instance.label}>
    <InstanceProvider instance={instance}>{instance.mode==='anonymous'?<SingleApp onState={report}/>:<AutomationProvider><SingleApp onState={report}/></AutomationProvider>}</InstanceProvider></section>)}</div>;
}
const archiveRoute=window.location.pathname.match(/^(.*\/instances\/[^/]+\/)rooms\/(\d+)\/$/);
createRoot(document.getElementById('root')!).render(<React.StrictMode>{archiveRoute?
  <RoomArchive base={archiveRoute[1]} roomId={archiveRoute[2]}/>:<App/>}</React.StrictMode>);
