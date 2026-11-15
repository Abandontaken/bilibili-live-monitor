import React,{useEffect,useRef,useState} from 'react';
import {useArchiveData} from './useArchiveData';
import type {PeriodSummary} from './PeriodOverview';

const number=(value:unknown)=>typeof value==='number'&&Number.isFinite(value)?value.toLocaleString('zh-CN',{maximumFractionDigits:1}):'—';
export function RoomPeriodMetrics({base,roomId}:{base:string;roomId:string}){
 const ref=useRef<HTMLDivElement>(null),[visible,setVisible]=useState(false);
 useEffect(()=>{const observer=new IntersectionObserver(entries=>{if(entries[0].isIntersecting){setVisible(true);observer.disconnect()}},{rootMargin:'100px'});
  if(ref.current)observer.observe(ref.current);return()=>observer.disconnect()},[]);
 return <div ref={ref} className="room-period">{visible?<Metrics base={base} roomId={roomId}/>:<p>最近 7 天 · —</p>}</div>;
}
function Metrics({base,roomId}:{base:string;roomId:string}){
 const {data,error}=useArchiveData<{periods:PeriodSummary[]}>(base+'api/rooms/'+roomId+'/archive?overview=true',300000);
 const p=data?.periods?.find(p=>p.days===7);
 return <><div className="compact-values">
  <div><span>日均来访人次</span><strong>{number(p?.entries_daily_mean)}</strong></div>
  <div><span>礼物价值</span><strong>{p?.gift_value_yuan==null?'—':'¥'+number(p.gift_value_yuan)}{p?.gift_amount_unknown&&p.gift_value_yuan!=null?'+':''}</strong></div>
  <div><span>粉丝团日均</span><strong>{number(p?.fans_club_daily_mean)}</strong><small>{p?.fans_club_latest!=null?`最新 ${number(p.fans_club_latest)} · ${p.fans_club_latest_date}`:'—'}</small></div>
  <div><span>粉丝团变化</span><strong>{p?.fans_club_change!=null&&p.fans_club_change>0?'+':''}{number(p?.fans_club_change)}</strong><small>{p?.fans_club_change_since?'较 '+p.fans_club_change_since:'—'}</small></div>
 </div>{p?<p className="period-range">{p.start} 至 {p.end} · 北京时间 · 含今天<br/>来访有记录 {p.observed_days} 天{p.gift_amount_unknown?' · 部分价值未知':''}</p>:<p role="status">{error?'归档暂时无法读取':'正在读取最近 7 天…'}</p>}
 {p&&error&&<p role="status">归档暂时无法读取 · 显示上次结果</p>}
 {p?.gift_share&&<p>{p.gift_share.label}送礼占比 {number(p.gift_share.percentage)}%</p>}
 </>;
}
