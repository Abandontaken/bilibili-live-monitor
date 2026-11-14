import React, {useEffect, useState} from 'react';
import {WelcomeCopy} from './WelcomeCopy';
import {useInstance} from './Instance';
import {CopyLikeScript} from './CopyLikeScript';

type Room = {room_id:string;label:string;intro:string;enabled:boolean;confirmed:string|null;delivery:string;error?:string};
type Like = {id:string;room_id:string;requested:number;triggered:number;status:string};
type State = {account:{mode?:string;managed:boolean;active:string;profiles:{id:string;label:string}[];logged_in:boolean;name?:string;switching:boolean};rooms:Room[];likes:Like[];notifications:{id:string;label:string;event:string;status:string}[]};
async function api(base:string,path:string,body?:unknown,method='POST'){
  const response=await fetch(base+'api/automation'+path,body===undefined?undefined:{method,headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const value=await response.json();
  if(!response.ok)throw new Error(typeof value.detail==='string'?value.detail:JSON.stringify(value.detail));
  return value;
}

type CardRoom={room_id:string;title:string;url:string;home_url?:string;status:string;recording_status:string};
export function AutomationControls({rooms:cards}:{rooms?:CardRoom[]}){
  const {base}=useInstance();
  const [state,setState]=useState<State|null>(null);
  const [error,setError]=useState('');
  const [busy,setBusy]=useState(false);
  const [profile,setProfile]=useState('');
  const [counts,setCounts]=useState<Record<string,number>>({});
  const [platformError,setPlatformError]=useState('');
  const [platform,setPlatform]=useState<{policy?:{enabled:boolean;fixed_rooms?:string[]};candidates:{room_id:string;title:string;url:string;watch_state?:string;blocked?:boolean;blocked_alias?:boolean;last_error?:string;status_error?:string}[];sources:{room_id:string;recording_status:string}[]}|null>(null);
  useEffect(()=>{let alive=true;let timer:ReturnType<typeof setTimeout>;
    async function refresh(){await Promise.all([
      api(base,'').then(next=>{if(alive){setState(next);setError('');}}).catch(e=>{if(alive)setError(e.message)}),
      fetch(base+'api/platform').then(async response=>{if(!response.ok)throw new Error('守候状态暂不可用');return response.json();})
        .then(next=>{if(alive){setPlatform(next);setPlatformError('');}}).catch(e=>{if(alive)setPlatformError(e.message)})]);
      if(alive)timer=setTimeout(()=>void refresh(),2000);}
    void refresh();return()=>{alive=false;clearTimeout(timer)}},[]);
  async function act(path:string,body:unknown,method='POST'){
    setBusy(true);setError('');try{await api(base,path,body,method);setState(await api(base,''))}
    catch(e){setError((e as Error).message)}finally{setBusy(false)}
  }
  if(!state)return error?<p role="alert">自动化状态：{error}</p>:null;
  const anonymous=state.account.mode==='anonymous';
  const entries=cards?cards.map(card=>({card,room:state.rooms.find(r=>r.room_id===card.room_id),
    candidate:platform?.candidates?.find(c=>c.room_id===card.room_id||c.url===card.url)})):
    state.rooms.map(room=>({room,card:undefined,candidate:platform?.candidates?.find(c=>c.room_id===room.room_id)}));
  return <section className="automation-panel" aria-label="直播自动化控制">
    <h2>{anonymous?'匿名监控 · 固定房间':'固定房间'}</h2>
    {!anonymous&&state.account.managed&&<details><summary>账号设置</summary><div className="automation-actions">
      <span>当前：{state.account.profiles.find(p=>p.id===state.account.active)?.label} · {state.account.logged_in?`已登录 ${state.account.name||''}`:'未登录，自动发送暂停'}</span>
      <select aria-label="账号资料" value={profile||state.account.active} disabled={busy||state.account.switching} onChange={e=>setProfile(e.target.value)}>
        {state.account.profiles.map(p=><option key={p.id} value={p.id}>{p.label}</option>)}
      </select>
      <button disabled={busy||state.account.switching||(profile||state.account.active)===state.account.active} onClick={()=>void act('/account',{profile:profile||state.account.active})}>切换账号</button>
      <button disabled={busy||state.account.switching} onClick={()=>void act('/account/login',{})}>打开登录窗口</button>
    </div><p className="footnote">切换账号会短暂停止采集；各账号登录资料分别保留。</p></details>}
    {entries.map(({room,card,candidate})=>{const id=card?.room_id||room!.room_id;
      const running=state.likes.find(x=>x.room_id===id&&x.status==='running');
      const latest=state.likes.filter(x=>x.room_id===id).at(-1);
      const issue=candidate?.blocked||candidate?.blocked_alias||candidate?.status_error||card?.status==='error';
      const status=issue?'需人工处理':platform?.policy?.enabled===false?'监控已暂停':
        card?.recording_status==='recording'?'正在录制':candidate?.watch_state==='retrying'?'录制恢复中':
        candidate?.watch_state==='confirming_offline'?'正在确认下播':room?.confirmed==='live'?'已开播':room?.confirmed==='offline'?'等待开播':'待确认';
      return <article key={id}>
        <div className="fixed-room-heading"><h3>{room?.label||card?.title} <small>{status}</small></h3>
        {card&&<a href={'#room-'+base.replace(/\W/g,'-')+id}>定位卡片</a>}{card?.home_url&&<a href={base+card.home_url}>按日汇总 →</a>}</div>
        {issue&&<p role="status">{candidate?.last_error||candidate?.status_error||'请检查本房间状态'}</p>}
        {!anonymous&&room&&<>
        <button aria-pressed={room.enabled} disabled={busy} onClick={()=>void act('/rooms/'+room.room_id,{enabled:!room.enabled},'PUT')}>{room.enabled?'关闭自动欢迎':'开启自动欢迎'}</button>
        <details><summary>欢迎语与点赞</summary>
        <div className="automation-actions">
          <label>点赞次数 <input aria-label={room.label+'点赞次数'} type="number" min="1" max="1000" value={counts[room.room_id]??100} onChange={e=>setCounts({...counts,[room.room_id]:Number(e.target.value)})}/></label>
          <button disabled={busy||!!running||!state.account.logged_in||state.account.switching||room.confirmed!=='live'} onClick={()=>void act('/rooms/'+room.room_id+'/likes',{count:counts[room.room_id]??100,request_id:crypto.randomUUID()})}>执行点赞</button>
          <CopyLikeScript roomId={room.room_id} count={counts[room.room_id]??100}/>
          {running&&<button disabled={busy} onClick={()=>void act('/likes/'+running.id+'/stop',{})}>停止点赞</button>}
        </div>
        <p>{room.intro}</p>
        <WelcomeCopy roomId={room.room_id}/>
        <p className="footnote">欢迎：{room.delivery}{room.error?' · '+room.error:''}。{latest&&`点赞：${latest.status}，前端触发 ${latest.triggered}/${latest.requested} 次（非服务器计入数）。`}</p>
        </details></>}
      </article>})}
    {state.notifications.length>0&&<p className="footnote">最近通知：{state.notifications.slice(-4).map(n=>`${n.label} ${n.event==='live'?'开播':'下播'} · ${n.status}`).join('；')}</p>}
    {error&&<p role="alert">{error}</p>}
    {platformError&&<p role="alert">{platformError}；欢迎控制仍可使用。</p>}
  </section>;
}
