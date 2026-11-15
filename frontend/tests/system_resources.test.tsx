import React from 'react';
import {afterEach,expect,it,vi} from 'vitest';
import {act,cleanup,render,screen} from '@testing-library/react';
import {SystemResources} from '../src/SystemResources';

const sample=()=>({source:'yamon',scope:'system',epoch:Date.now()/1000,cpu_percent:25,
  memory_used_gib:16,memory_total_gib:64,gpu_percent:40,download_mbps:16,upload_mbps:1});
afterEach(()=>{cleanup();vi.unstubAllGlobals();vi.useRealTimers()});

it('整机口径、单位与已用/总量明确，不使用监控进程标签',async()=>{
 const fetch=vi.fn().mockResolvedValue({ok:true,json:async()=>sample()});vi.stubGlobal('fetch',fetch);
 render(<SystemResources/>);
 expect(await screen.findByText('25.0')).toBeTruthy();
 expect(screen.getByText('16.0 / 64')).toBeTruthy();
 expect(screen.getByText('16.0 / 1.0')).toBeTruthy();
 expect(screen.queryByText(/监控 RSS/)).toBeNull();
 expect(screen.getByText(/包含其他应用开销/)).toBeTruthy();
 expect(fetch.mock.calls[0][0]).toBe('/api/dashboard/resources');
});

it('过期采样不显示为当前数据，缺失 GPU 不填零',async()=>{
 vi.stubGlobal('fetch',vi.fn().mockResolvedValue({ok:true,json:async()=>({...sample(),epoch:Date.now()/1000-60,gpu_percent:null})}));
 render(<SystemResources/>);
 expect(await screen.findByText('采样数据滞后')).toBeTruthy();
 expect(screen.queryByText('25.0')).toBeNull();
 expect(screen.queryByText('0')).toBeNull();
});

it('上游失败清除旧数值并在下次采样恢复',async()=>{
 vi.useFakeTimers();
 const fetch=vi.fn().mockResolvedValue({ok:true,json:async()=>sample()});vi.stubGlobal('fetch',fetch);
 render(<SystemResources/>);await act(async()=>{});
 expect(screen.getByText('25.0')).toBeTruthy();
 fetch.mockResolvedValueOnce({ok:false});
 await act(async()=>{await vi.advanceTimersByTimeAsync(5000)});
 expect(screen.getByText('YAMON 采样暂不可用')).toBeTruthy();
 expect(screen.queryByText('25.0')).toBeNull();
 await act(async()=>{await vi.advanceTimersByTimeAsync(5000)});
 expect(screen.getByText('25.0')).toBeTruthy();
});

it('响应头已到但正文挂起时也在四秒内取消，随后继续刷新',async()=>{
 vi.useFakeTimers();
 const fetch=vi.fn().mockImplementation((_url,options)=>Promise.resolve({ok:true,
  json:()=>new Promise((_resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new Error('aborted'))))}));
 vi.stubGlobal('fetch',fetch);render(<SystemResources/>);await act(async()=>{});
 await act(async()=>{await vi.advanceTimersByTimeAsync(4000)});
 expect(screen.getByText('YAMON 采样暂不可用')).toBeTruthy();
 fetch.mockResolvedValueOnce({ok:true,json:async()=>sample()});
 await act(async()=>{await vi.advanceTimersByTimeAsync(5000)});
 expect(screen.getByText('25.0')).toBeTruthy();
});
