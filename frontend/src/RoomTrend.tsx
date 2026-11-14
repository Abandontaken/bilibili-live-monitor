import React,{useEffect,useRef,useState} from 'react';
import {useArchiveData} from './useArchiveData';
import {SessionOverviewChart} from './SessionOverviewChart';
import type {SummaryRow} from './SessionSummaryTable';

export function RoomTrend({base,roomId}:{base:string;roomId:string}){
  const ref=useRef<HTMLDivElement>(null);
  const [visible,setVisible]=useState(false);
  useEffect(()=>{
    if(typeof IntersectionObserver==='undefined'){setVisible(true);return;}
    const observer=new IntersectionObserver(entries=>{if(entries[0].isIntersecting){setVisible(true);observer.disconnect();}},{rootMargin:'100px'});
    if(ref.current)observer.observe(ref.current);return()=>observer.disconnect();
  },[]);
  return <div className="room-trend" ref={ref}>{visible?<TrendData base={base} roomId={roomId}/>:<p>场次趋势</p>}</div>;
}
function TrendData({base,roomId}:{base:string;roomId:string}){
  const {data,error}=useArchiveData<{summaries?:SummaryRow[];updated_at?:string}>(base+'api/rooms/'+roomId+'/archive?trend=true',300000);
  return <>
    {data?<SessionOverviewChart rows={data.summaries||[]}/>:<p>{error?'场次趋势暂不可用':'正在读取场次趋势…'}</p>}
    {data&&error&&<p role="status">趋势刷新失败，显示上次结果{data.updated_at?' · 数据时间 '+new Date(data.updated_at).toLocaleString('zh-CN'):''}</p>}
  </>;
}
