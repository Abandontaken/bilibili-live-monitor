import {useEffect,useState} from 'react';

let running=0;
const waiting:(()=>void)[]=[];
async function archiveFetch(url:string,controller:AbortController){
 await new Promise<void>(resolve=>{const start=()=>{running++;resolve()};if(running<2)start();else waiting.push(start)});
 const timeout=setTimeout(()=>controller.abort(),60000);
 try{controller.signal.throwIfAborted();return await fetch(url,{cache:'no-store',signal:controller.signal});}
 finally{clearTimeout(timeout);running--;waiting.shift()?.();}
}

export function useArchiveData<T>(url:string,interval:number,initial:T|null=null,validate?:(value:unknown)=>boolean){
 const [data,setData]=useState<T|null>(initial),[error,setError]=useState('');
 useEffect(()=>{
  let alive=true,busy=false,timer:ReturnType<typeof setTimeout>|undefined,controller:AbortController|undefined;
  const schedule=()=>{if(alive)timer=setTimeout(()=>void refresh(),interval)};
  async function refresh(){
   if(!alive||busy)return;
   clearTimeout(timer);
   if(document.hidden){schedule();return;}
   busy=true;controller=new AbortController();
   try{
     const response=await archiveFetch(url,controller);
    if(!response.ok)throw Error('归档暂时无法读取');
    const value=await response.json();
    if(validate&&!validate(value))throw Error('归档暂时无法读取');
    if(alive){setData(value);setError('');}
   }catch{if(alive)setError('归档暂时无法读取');}
    finally{busy=false;schedule();}
  }
  const visible=()=>{if(!document.hidden)void refresh()};
  document.addEventListener('visibilitychange',visible);
  void refresh();
  return()=>{alive=false;clearTimeout(timer);controller?.abort();document.removeEventListener('visibilitychange',visible)};
 },[url,interval,validate]);
 return {data,error};
}
