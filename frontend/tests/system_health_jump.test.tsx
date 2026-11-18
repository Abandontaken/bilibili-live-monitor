import React from 'react';
import {afterEach,expect,it,vi} from 'vitest';
import {cleanup,fireEvent,render,screen} from '@testing-library/react';
import {SystemHealth,roomAnchorId} from '../src/main';
import type {Instance} from '../src/Instance';

afterEach(()=>{cleanup();vi.unstubAllGlobals()});

it('展开当前录制抽屉并在点击主播标签时派发精确锚点定位事件',async()=>{
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue({ok:false}));
  const instances:Instance[]=[
    {id:'account',label:'账号监控',mode:'account',base:'/instances/account/'},
    {id:'anonymous',label:'匿名监控',mode:'anonymous',base:'/instances/anonymous/'},
  ];
  const makeRoom=(roomId:string,name:string,status:string,recStatus:string)=>({
    room_id:roomId,title:name,display_name:name,url:'https://live.bilibili.com/'+roomId,cover_url:'',
    status,fixed_task:true,record_mode:'audio' as const,asr_enabled:false,preview_enabled:false,
    recording_status:recStatus,recorded_seconds:780,recording_files:3,recording_bytes:3200000,
    error:null,chunk_seconds:5,segments:[],items:[],
  });
  const reports={
    account:{connected:true,state:{rooms:[makeRoom('100001','示例主播甲','waiting','stopped')],
      fixed_task_count:3,raw_fixed_count:3,queue_depth:0,queue_capacity:20,asr_ready:true,asr_enabled:true,
      overload_count:0,inflight:null,active_count:0,pending_count:0,asr_restarts:0,asr_retries:0,asr_error:null}},
    anonymous:{connected:true,state:{rooms:[
      makeRoom('100002','示例主播乙','capturing','recording'),
      makeRoom('100003','示例主播丙','capturing','recording'),
    ],fixed_task_count:20,raw_fixed_count:20,queue_depth:0,queue_capacity:20,asr_ready:false,asr_enabled:false,
      overload_count:0,inflight:null,active_count:2,pending_count:0,asr_restarts:0,asr_retries:0,asr_error:null}},
  };

  const events:Array<{instanceId:string;anchorId:string}>=[];
  const listener=(e:Event)=>events.push((e as CustomEvent).detail);
  window.addEventListener('bili-focus-room',listener);
  try{
    render(<SystemHealth instances={instances} reports={reports}/>);
    const toggle=screen.getByRole('button',{name:/录制 2/});
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(screen.queryByRole('region',{name:'正在采集的直播间'})).toBeNull();

    fireEvent.click(toggle);
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    const link=screen.getByRole('link',{name:/示例主播乙/});
    const expectedAnchor=roomAnchorId('/instances/anonymous/','100002');
    expect(link.getAttribute('href')).toBe('#'+expectedAnchor);

    fireEvent.click(link);
    expect(events).toEqual([{instanceId:'anonymous',anchorId:expectedAnchor}]);
  }finally{
    window.removeEventListener('bili-focus-room',listener);
  }
});
