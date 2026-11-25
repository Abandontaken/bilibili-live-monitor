import React,{useEffect,useState} from 'react';
import './room_archive.css';
import type {SummaryRow} from './SessionSummaryTable';
import type {PeriodSummary,GiftShare} from './PeriodOverview';
import {ArchiveOverview} from './ArchiveOverview';
import {ArchiveHeader} from './ArchiveHeader';
import {useArchiveData} from './useArchiveData';
import {remoteArchiveUrl} from './remoteArchive';

type Summary={followers_change:number|null;gift_count:number|null;gift_value_yuan?:number|null;watched:number|null};
type Session={key:string;start:string;end:string;status:string;has_result:boolean;summary:Summary;dates:string[]};
type Day={date:string;session_keys:string[];observed_seconds:number;coverage_seconds:number;danmaku:number|null;entries:number|null;
  gift_count:number|null;gift_value_yuan?:number|null;gift_count_unknown:number;gift_amount_unknown:number;followers_change:number|null;followers_partial:boolean;has_observations:boolean;
  watched_sessions?:{key:string;start:string;value:number|null;observed_at:string|null}[];
  fans_club_close?:number|null;fans_club_close_at?:string|null;fans_club_change?:number|null;fans_club_change_since?:string|null};
type Archive={room_id:string;name:string;mode?:string;timezone:string;sessions:Session[];summaries?:SummaryRow[];summary_source?:string;days:Day[];warnings:number;updated_at:string|null;periods?:PeriodSummary[];gift_share?:GiftShare};
const clock=(value:string)=>new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});
const number=(n:number|null|undefined)=>n==null?'—':n.toLocaleString('zh-CN');
const hours=(n:number)=>(n/3600).toFixed(2)+' 小时';
const status:Record<string,string>={completed:'已完成',live:'直播中',created:'准备中',running:'处理中',stopping:'收尾中',sealing:'收尾中',offline_asr:'离线转写',offline_review:'最终分析',finalizing:'生成最终结果',attention:'需处理'};

export function RoomArchive({base,roomId}:{base:string;roomId:string}){
 const remoteUrl=remoteArchiveUrl(roomId);
 const {data,error}=useArchiveData<Archive>(base+'api/rooms/'+roomId+'/archive',30000);
 const [date,setDate]=useState('');
  useEffect(()=>{if(!data)return;document.title=data.name+' · 直播场次汇总';
   for(const [key,value] of [['description',data.name+'的直播场次汇总：平均在线、峰值在线、累计看过、关注变化与礼物统计。'],['robots','noindex,nofollow']]){
    let meta=document.querySelector<HTMLMetaElement>('meta[name="'+key+'"]');
    if(!meta){meta=document.createElement('meta');meta.name=key;document.head.append(meta);}meta.content=value;
   }
  },[data?.name]);
 const selected=data?.days.find(d=>d.date===date)||data?.days[0];
 const sessions=data?.sessions.filter(s=>selected?.session_keys.includes(s.key))||[];
 return <main className="room-archive">
   <a href={import.meta.env.BASE_URL}>← 返回直播看板</a>
    <ArchiveHeader title={data?.name||'房间 '+roomId} links={[{label:'打开直播间 ↗',href:'https://live.bilibili.com/'+roomId,external:true},...(remoteUrl?[{label:'远程逐日看板 ↗',href:remoteUrl,external:true}]:[])]} rows={data?.summaries}/>
  {error&&<p role="alert">{error}；已有内容保留，稍后自动重试。</p>}
  {!data&&!error&&<p>正在读取历史场次…</p>}
     {data&&<>
    <ArchiveOverview data={data} rows={data.summaries||[]} base={base}/>
   {selected&&<>
    <div className="archive-section-title"><h2>{selected.date}</h2><label>选择日期 <select aria-label="选择日期" value={selected.date} onChange={e=>setDate(e.target.value)}>{data.days.map(d=><option key={d.date}>{d.date}</option>)}</select></label></div>
    <section className="archive-stats">
     <div><span>当天涉及场次</span><strong>{selected.session_keys.length}</strong></div>
      <div><span>已记录时长</span><strong>{hours(selected.coverage_seconds)}</strong><small>直播时长 {hours(selected.observed_seconds)}</small></div>
     <div><span>弹幕条数</span><strong>{number(selected.danmaku)}</strong></div>
      <div><span>进房次数</span><strong>{number(selected.entries)}</strong></div>
     <div><span>累计看过（分场）</span>{selected.watched_sessions?.length?selected.watched_sessions.map(w=><section key={w.key}>
      <strong>{number(w.value)}</strong><small style={{display:'block'}}>开播 {clock(w.start)}</small>
     </section>):<strong>—</strong>}</div>
     <div><span>礼物价值</span><strong>{selected.gift_value_yuan==null?'—':'¥'+selected.gift_value_yuan.toFixed(2)}{selected.gift_amount_unknown?'+':''}</strong><small>{number(selected.gift_count)} 件</small></div>
      {'fans_club_close' in selected&&<div><span>粉丝团人数</span><strong>{number(selected.fans_club_close)}</strong>
       {selected.fans_club_close!=null&&selected.fans_club_change!=null&&selected.fans_club_change_since&&
        <small>较 {selected.fans_club_change_since.slice(5)} {selected.fans_club_change>0?'+':''}{number(selected.fans_club_change)}</small>}</div>}
      <div><span>净涨粉</span><strong>{selected.followers_change==null?'—':(selected.followers_change>0?'+':'')+number(selected.followers_change)}</strong></div>
    </section>
    <h2>当天场次</h2><section className="archive-sessions">{sessions.map(s=><article className="archive-panel" key={s.key}>
     <div className="archive-section-title"><h3>{clock(s.start)} — {clock(s.end)}</h3><span className={'archive-status '+s.status}>{status[s.status]||'处理中'}</span></div>
     <p>本场礼物价值 ¥{s.summary.gift_value_yuan?.toFixed(2)??'—'} · {number(s.summary.gift_count)} 件 · 平台累计看过 {number(s.summary.watched)}</p>
     {s.has_result?<a className="archive-result" href={base+'sessions/'+s.key+'/'}>{s.status==='completed'?'查看最终结果':'查看本场实时结果'} →</a>:<p>本场结果准备中</p>}
    </article>)}</section>
   </>}
    {!!data.warnings&&<p role="alert">有 {data.warnings} 场记录暂时无法显示。</p>}
   <p className="archive-note">最新数据时间：{data.updated_at?clock(data.updated_at):'暂无'}</p>
  </>}
 </main>;
}
