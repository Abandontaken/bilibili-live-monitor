import React from 'react';
import {afterEach,expect,it,vi} from 'vitest';
import {createRoot} from 'react-dom/client';
import {act} from 'react';
import {RoomTrend} from '../src/RoomTrend';

afterEach(()=>vi.unstubAllGlobals());
it('首页趋势读取对应实例和房间的场次汇总',async()=>{
  const fetcher=vi.fn().mockResolvedValue({ok:true,json:async()=>({summaries:[]})});
  vi.stubGlobal('fetch',fetcher);
  const node=document.createElement('div');document.body.append(node);
  const root=createRoot(node);
  await act(async()=>root.render(<RoomTrend base="/instances/guest/" roomId="123456"/>));
  expect(fetcher.mock.calls[0][0]).toBe('/instances/guest/api/rooms/123456/archive?trend=true');
  expect(node.textContent).toContain('场次趋势');
  expect(node.textContent).toContain('暂无场次数据');
  await act(async()=>root.unmount());node.remove();
});
