/* ============================================================
   sync.js — 个人看板云同步引擎 (Supabase)
   在应用内填入 Supabase 配置即可启用，未配置时纯本地运行。
   同步策略：每键 last-write-wins；实时订阅 + 定时拉取 + 焦点拉取。
   ============================================================ */
(function () {
  'use strict';

  // 需要同步的 localStorage 键（任务 + 日记 + 归物 + 点评 + 健康 + 垃圾桶）
  var SYNC_KEYS = ['ticktick_pro_state', 'dayone_premium_diaries', 'asset_tracker_state', 'reviews_pro_state', 'health_pro_state', 'trash_pro_state'];
  var CONFIG_KEY = 'pb_sync_config';   // { url, anonKey }
  var CODE_KEY   = 'pb_sync_code';     // 同步码（房间号）
  var META_KEY   = 'pb_sync_meta';     // { key: 已同步时间戳ms }

  var sb = null;            // Supabase 客户端
  var lastSnapshot = {};    // 内存快照（防重复推送 / 推送循环）
  var pushTimer = null;
  var pullTimer = null;
  var pushCheckTimer = null; // 定时推送检查（storage 事件兜底）
  var realtimeCh = null;
  var rtPullTimer = null;   // Realtime 回调防抖
  var connected = false;
  var started = false;
  var lastError = '';       // 最后一次错误信息
  var firstPullDone = false; // 首次拉取完成前禁止推送（防空数据覆盖云端）
  var pushing = false;      // 推送锁，防止并发 doPush 竞争

  /* ---------- 配置存取 ---------- */
  function loadConfig() { try { return JSON.parse(localStorage.getItem(CONFIG_KEY) || '{}'); } catch (e) { return {}; } }
  function saveConfig(c) { localStorage.setItem(CONFIG_KEY, JSON.stringify(c)); }
  function isConfigured() { var c = loadConfig(); return !!(c.url && c.anonKey); }

  function genCode(len) {
    var chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 去掉易混字符
    var s = '';
    for (var i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
    return s;
  }
  // 由 Supabase 配置确定性派生房间码：同一项目(同配置)的所有设备自动落入同一房间，
  // 彻底消除"各设备各自生成随机码 → 不同房间 → 只能同步一次 / 手机不能同步"的顽疾。
  function deterministicCode() {
    var cfg = loadConfig();
    var seed = '';
    if (cfg.url) { var m = String(cfg.url).match(/https?:\/\/([^.]+)\.supabase\.co/); seed = m ? m[1] : String(cfg.url); }
    if (!seed && cfg.anonKey) seed = cfg.anonKey;
    if (!seed) seed = 'default';
    var h = 2166136261 >>> 0; // FNV-1a
    for (var i = 0; i < seed.length; i++) { h ^= seed.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
    var chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    var out = '', x = h;
    for (var j = 0; j < 8; j++) { out += chars[x % chars.length]; x = (Math.imul(x, 31) + 7) >>> 0; }
    return 'PB' + out;
  }
  // 仅在用户手动「加入其他房间 / 重新生成同步码」时打 manual 标记，避免被确定性码覆盖
  function getCode() {
    if (localStorage.getItem('pb_sync_manual')) {
      var mc = localStorage.getItem(CODE_KEY);
      if (mc) return mc;
    }
    var d = deterministicCode();
    localStorage.setItem(CODE_KEY, d);
    return d;
  }
  function setCode(c, manual) {
    localStorage.setItem(CODE_KEY, c);
    if (manual) localStorage.setItem('pb_sync_manual', '1');
    else localStorage.removeItem('pb_sync_manual');
  }

  /* ---------- 房间迁移：把旧房间(随机码)的数据并入确定性房间，保留历史数据 ---------- */
  async function migrateToRoom(targetCode) {
    try {
      if (!sb) return;
      // 目标房间已有数据则无需迁移
      var t = await sb.from('dashboard_sync').select('data_key').eq('sync_code', targetCode).limit(1);
      if (t.data && t.data.length) return;
      // 找最近的非目标房间
      var rooms = await sb.from('dashboard_sync').select('sync_code,updated_at').order('updated_at', { ascending: false }).limit(100);
      if (!rooms.data || !rooms.data.length) return;
      var srcCode = null;
      for (var i = 0; i < rooms.data.length; i++) {
        if (rooms.data[i].sync_code !== targetCode) { srcCode = rooms.data[i].sync_code; break; }
      }
      if (!srcCode) return;
      var rows = await sb.from('dashboard_sync').select('data_key,data_value,updated_at').eq('sync_code', srcCode);
      if (!rows.data || !rows.data.length) return;
      var tasks = rows.data.map(function (r) {
        return sb.from('dashboard_sync').upsert(
          { sync_code: targetCode, data_key: r.data_key, data_value: r.data_value, updated_at: r.updated_at },
          { onConflict: 'sync_code,data_key' }
        );
      });
      await Promise.all(tasks);
      console.log('[Sync] 已迁移房间 ' + srcCode + ' → ' + targetCode + ' (' + rows.data.length + ' 行)');
    } catch (e) { console.warn('[Sync] 房间迁移失败(可忽略，后台会重试):', e.message || e); }
  }

  function getMeta() { try { return JSON.parse(localStorage.getItem(META_KEY) || '{}'); } catch (e) { return {}; } }
  function setMeta(m) { localStorage.setItem(META_KEY, JSON.stringify(m)); }

  function snapshotLocal() {
    var s = {};
    SYNC_KEYS.forEach(function (k) { s[k] = localStorage.getItem(k); });
    return s;
  }

  /* ---------- 加载 Supabase 库 ---------- */
  function loadLib() {
    if (window.supabase) return Promise.resolve();
    return new Promise(function (resolve, reject) {
      var sc = document.createElement('script');
      sc.src = 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.0/dist/umd/supabase.min.js';
      sc.timeout = 15000;
      sc.onload = resolve;
      sc.onerror = function () {
        // jsdelivr 失败时尝试 unpkg 备用源
        var sc2 = document.createElement('script');
        sc2.src = 'https://unpkg.com/@supabase/supabase-js@2.45.0/dist/umd/supabase.min.js';
        sc2.timeout = 15000;
        sc2.onload = resolve;
        sc2.onerror = function () { reject(new Error('Supabase 库加载失败，请检查网络是否能访问 cdn.jsdelivr.net')); };
        document.head.appendChild(sc2);
      };
      document.head.appendChild(sc);
    });
  }

  async function ensureClient() {
    if (sb) return sb;
    var cfg = loadConfig();
    if (!cfg.url || !cfg.anonKey) return null;
    try {
      await loadLib();
    } catch (e) {
      lastError = '库加载失败: ' + e.message;
      updateStatusBadge();
      throw e;
    }
    sb = window.supabase.createClient(cfg.url, cfg.anonKey, { realtime: { params: { eventsPerSecond: 10 } } });
    return sb;
  }

  /* ---------- 拉取（云端较新则覆盖本地）---------- */
  async function pullAll() {
    var client = await ensureClient();
    if (!client) return { ok: false, reason: 'not-configured' };
    try {
      var res = await client.from('dashboard_sync')
        .select('data_key,data_value,updated_at')
        .eq('sync_code', getCode());
      if (res.error) throw res.error;
      var meta = getMeta();
      var changed = false;
      (res.data || []).forEach(function (row) {
        var cloudTs = new Date(row.updated_at).getTime();
        var localTs = meta[row.data_key] || 0;
        if (cloudTs > localTs) {
          var val = (typeof row.data_value === 'string') ? row.data_value : JSON.stringify(row.data_value);
          localStorage.setItem(row.data_key, val);
          meta[row.data_key] = cloudTs;
          lastSnapshot[row.data_key] = val;
          changed = true;
        }
      });
      if (changed) {
        setMeta(meta);
        notifyIframes();
        if (typeof currentView !== 'undefined' && currentView === 'dashboard' && window.renderDashboard) window.renderDashboard();
      }
      connected = true;
      lastError = '';
      updateStatusBadge();
      return { ok: true, changed: changed };
    } catch (e) {
      connected = false;
      lastError = e.message || String(e);
      updateStatusBadge();
      return { ok: false, reason: e.message };
    }
  }

  /* ---------- 推送（本地有变化则上传）---------- */
  function schedulePush() {
    if (!isConfigured()) return;
    if (!firstPullDone) return; // 首次拉取完成前禁止推送，防空数据覆盖云端
    clearTimeout(pushTimer);
    pushTimer = setTimeout(doPush, 500);
  }

  async function doPush() {
    if (!firstPullDone) return { ok: false, reason: 'waiting-first-pull' };
    if (pushing) return { ok: false, reason: 'push-in-progress' };
    pushing = true;
    try {
      var client = await ensureClient();
      if (!client) return { ok: false };
      var snap = snapshotLocal();
      var meta = getMeta();
      var code = getCode();
      var tasks = [];
      SYNC_KEYS.forEach(function (k) {
        // 推送条件：与上次快照不同，或从未同步过
        if (snap[k] !== lastSnapshot[k] || meta[k] === undefined) {
          // 空数据保护：本地为空但云端曾有数据时，跳过此键，避免误删云端
          var isEmpty = !snap[k] || snap[k] === 'null' || snap[k] === '[]' || snap[k] === '{}';
          if (isEmpty && meta[k]) {
            console.warn('[Sync] 跳过推送空数据(键=' + k + ')，防止覆盖云端');
            lastSnapshot[k] = snap[k]; // 标记已确认，避免重复检测
            return;
          }
          var value;
          try { value = JSON.parse(snap[k]); } catch (e) { value = snap[k]; }
          var ts = new Date().toISOString();
          (function (key, val, timestamp) {
            tasks.push(
              client.from('dashboard_sync').upsert(
                { sync_code: code, data_key: key, data_value: val, updated_at: timestamp },
                { onConflict: 'sync_code,data_key' }
              ).then(function () {
                meta[key] = new Date(timestamp).getTime();
                lastSnapshot[key] = snap[key];
              })
            );
          })(k, value, ts);
        }
      });
      if (tasks.length === 0) return { ok: true, nothing: true };
      try {
        await Promise.all(tasks);
        setMeta(meta);
        connected = true;
        updateStatusBadge();
        return { ok: true };
      } catch (e) {
        connected = false;
        updateStatusBadge();
        return { ok: false, reason: e.message };
      }
    } finally {
      pushing = false;
    }
  }

  /* ---------- 强制推送全部数据（绕过所有保护，确保两个模块都上传）---------- */
  async function forcePushAll() {
    if (pushing) return { ok: false, reason: 'push-in-progress' };
    pushing = true;
    try {
      var client = await ensureClient();
      if (!client) return { ok: false };
      var snap = snapshotLocal();
      var code = getCode();
      var meta = {};
      var tasks = [];
      var pushedKeys = [];
      SYNC_KEYS.forEach(function (k) {
        if (!snap[k]) {
          console.warn('[Sync] forcePush: 键 ' + k + ' 本地为空，跳过');
          return;
        }
        var value;
        try { value = JSON.parse(snap[k]); } catch (e) { value = snap[k]; }
        var ts = new Date().toISOString();
        (function (key, val, timestamp) {
          tasks.push(
            client.from('dashboard_sync').upsert(
              { sync_code: code, data_key: key, data_value: val, updated_at: timestamp },
              { onConflict: 'sync_code,data_key' }
            ).then(function () {
              meta[key] = new Date(timestamp).getTime();
              lastSnapshot[key] = snap[key];
              pushedKeys.push(key);
              console.log('[Sync] forcePush: 已推送 ' + key + ' (' + snap[key].length + ' 字符)');
            })
          );
        })(k, value, ts);
      });
      if (tasks.length === 0) {
        pushing = false;
        return { ok: false, reason: '本地无数据可推送' };
      }
      try {
        await Promise.all(tasks);
        setMeta(meta);
        firstPullDone = true;
        connected = true;
        updateStatusBadge();
        console.log('[Sync] forcePush 完成，已推送 ' + pushedKeys.length + ' 个键: ' + pushedKeys.join(', '));
        return { ok: true, keys: pushedKeys };
      } catch (e) {
        connected = false;
        updateStatusBadge();
        return { ok: false, reason: e.message };
      }
    } finally {
      pushing = false;
    }
  }

  /* ---------- 通知 iframe 刷新 ---------- */
  function notifyIframes() {
    // 发送 3 次（0ms / 200ms / 600ms），确保 iframe 已就绪时能收到
    [0, 200, 600].forEach(function (delay) {
      setTimeout(function () {
        ['frame-tasks', 'frame-diary', 'frame-assets', 'frame-reviews', 'frame-health', 'frame-trash'].forEach(function (id) {
          var f = document.getElementById(id);
          if (f && f.contentWindow) { try { f.contentWindow.postMessage({ type: 'pb-sync-update' }, '*'); } catch (e) {} }
        });
      }, delay);
    });
  }

  /* ---------- 强制刷新指定 iframe（切 tab 时调用）---------- */
  function refreshIframe(frameId) {
    var f = document.getElementById(frameId);
    if (f && f.contentWindow) { try { f.contentWindow.postMessage({ type: 'pb-sync-update' }, '*'); } catch (e) {} }
  }

  /* ---------- 实时订阅（即时同步）---------- */
  async function startRealtime() {
    var client = await ensureClient();
    if (!client || realtimeCh) return;
    var code = getCode();
    realtimeCh = client.channel('pb-sync-' + code)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'dashboard_sync', filter: 'sync_code=eq.' + code }, function () {
        // 防抖 300ms，避免短时间内多条变更触发多次 pullAll
        clearTimeout(rtPullTimer);
        rtPullTimer = setTimeout(pullAll, 300);
      })
      .subscribe();
  }

  /* ---------- storage 事件：iframe 写入 → 推送 ---------- */
  window.addEventListener('storage', function (e) {
    if (SYNC_KEYS.indexOf(e.key) === -1) return;
    schedulePush();
  });

  /* ---------- 状态徽标 ---------- */
  function updateStatusBadge() {
    var el = document.getElementById('sync-badge');
    if (!el) return;
    if (!isConfigured()) { el.textContent = '☁︎ 未开启'; el.className = 'text-[11px] font-semibold text-slate-400'; return; }
    if (connected) { el.textContent = '☁︎ 已同步'; el.className = 'text-[11px] font-semibold text-emerald-500'; return; }
    if (lastError) { el.textContent = '☁︎ 同步异常'; el.className = 'text-[11px] font-semibold text-red-500'; return; }
    el.textContent = '☁︎ 连接中…'; el.className = 'text-[11px] font-semibold text-amber-500';
  }

  /* ---------- 启动 ---------- */
  // pullOnly=true 表示只拉取不推送（用于加入新房间时）
  async function start(pullOnly) {
    if (started || !isConfigured()) return;
    started = true;
    lastSnapshot = {};
    firstPullDone = false;
    try {
      await ensureClient();
      // 房间码确定性收敛：同一 Supabase 配置 → 同一确定性房间码(PBxxxxxx)，
      // 旧房间(随机码)数据自动迁移过来，从根本上消除分房导致的"只能同步一次"。
      var targetCode = deterministicCode();
      var manualFlag = localStorage.getItem('pb_sync_manual');
      var currentCode = localStorage.getItem(CODE_KEY);
      if (!manualFlag && currentCode !== targetCode) {
        await migrateToRoom(targetCode);
        setCode(targetCode, false);
      }
      // 初始推送/拉取包在独立 try 中：即使瞬时失败，下方的定时轮询仍会持续重试恢复
      try {
        if (pullOnly) {
          // 加入新房间：只拉取云端数据，不推送本地
          await pullAll();
        } else {
          // 正常启动：先推送本地数据，再拉取云端
          await forcePushAll();   // 1. 先把本地两个模块的数据推到云端
          await pullAll();        // 2. 再拉取云端最新数据
        }
      } catch (e) {
        console.warn('[Sync] 初始推送/拉取失败，后台轮询将持续重试:', e.message || e);
      }
      firstPullDone = true;
      notifyIframes();
      pullTimer = setInterval(pullAll, 5000);  // 5秒轮询拉取
      // 定时推送检查：storage 事件在 iframe↔parent 间不可靠，用定时器兜底
      pushCheckTimer = setInterval(function () {
        var snap = snapshotLocal();
        var needPush = false;
        SYNC_KEYS.forEach(function (k) {
          if (snap[k] !== lastSnapshot[k]) needPush = true;
        });
        if (needPush) schedulePush();
      }, 4000);
      startRealtime();
      window.addEventListener('focus', function () { pullAll(); });
      document.addEventListener('visibilitychange', function () { if (!document.hidden) pullAll(); });
      updateStatusBadge();
    } catch (e) {
      // 连接失败时不放弃，每 15 秒重试一次
      lastError = e.message || String(e);
      updateStatusBadge();
      console.warn('[Sync] 启动失败，15秒后重试:', lastError);
      setTimeout(function () {
        started = false;
        sb = null;
        start();
      }, 15000);
    }
  }

  /* ============================================================
     设置弹窗 UI
     ============================================================ */
  function openSyncModal() {
    var m = document.getElementById('sync-modal');
    m.classList.remove('hidden'); m.classList.add('flex');
    renderModalBody();
  }
  function closeSyncModal() {
    var m = document.getElementById('sync-modal');
    m.classList.add('hidden'); m.classList.remove('flex');
  }

  function renderModalBody() {
    var body = document.getElementById('sync-modal-body');
    if (!isConfigured()) { body.innerHTML = notConfiguredHTML(); }
    else { body.innerHTML = configuredHTML(); }
  }

  function notConfiguredHTML() {
    return ''
    + '<div class="space-y-4">'
    +   '<div class="bg-indigo-50 border border-indigo-100 rounded-xl p-4 text-sm text-slate-600 leading-relaxed">'
    +     '<div class="font-bold text-indigo-600 mb-2">🚀 开启跨设备同步（约 3 分钟）</div>'
    +     '<ol class="list-decimal ml-5 space-y-1.5">'
    +       '<li>打开 <a href="https://supabase.com" target="_blank" class="text-indigo-600 font-semibold underline">supabase.com</a>，注册并新建一个项目（免费档即可，地区选最近的）</li>'
    +       '<li>项目创建后，进入左侧「SQL Editor」，粘贴下方 SQL 并运行：</li>'
    +     '</ol>'
    +     '<pre class="bg-slate-900 text-slate-100 text-[11px] rounded-lg p-3 mt-2 overflow-x-auto leading-relaxed">CREATE TABLE IF NOT EXISTS dashboard_sync (\n  sync_code TEXT NOT NULL,\n  data_key TEXT NOT NULL,\n  data_value JSONB,\n  updated_at TIMESTAMPTZ DEFAULT now(),\n  PRIMARY KEY (sync_code, data_key)\n);\nALTER TABLE dashboard_sync ENABLE ROW LEVEL SECURITY;\nCREATE POLICY "allow_all" ON dashboard_sync\n  FOR ALL USING (true) WITH CHECK (true);\nALTER PUBLICATION supabase_realtime ADD TABLE dashboard_sync;</pre>'
    +       '<li>进入「Project Settings → API」，复制 <b>Project URL</b> 和 <b>anon public key</b>，填入下方</li>'
    +     '</ol>'
    +   '</div>'
    +   '<div class="space-y-3">'
    +     '<div>'
    +       '<label class="text-xs font-bold text-slate-500 uppercase tracking-wide">Project URL</label>'
    +       '<input id="sb-url" type="text" placeholder="https://xxxx.supabase.co" class="w-full mt-1 px-3 py-2.5 rounded-lg border border-slate-200 text-sm focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100 outline-none">'
    +     '</div>'
    +     '<div>'
    +       '<label class="text-xs font-bold text-slate-500 uppercase tracking-wide">anon public key</label>'
    +       '<textarea id="sb-key" rows="3" placeholder="eyJhbGciOi..." class="w-full mt-1 px-3 py-2.5 rounded-lg border border-slate-200 text-xs font-mono focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100 outline-none resize-none"></textarea>'
    +     '</div>'
    +     '<button onclick="SyncEngine.saveAndConnect()" class="w-full py-3 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-sm transition shadow-lg shadow-indigo-200">连接并开启同步</button>'
    +   '</div>'
    + '</div>';
  }

  function configuredHTML() {
    var code = getCode();
    var statusText = connected ? '● 已连接' : (lastError ? '● 连接异常' : '● 连接中…');
    var statusClass = connected ? 'text-emerald-500' : (lastError ? 'text-red-500' : 'text-amber-500');
    var errorBlock = '';
    if (lastError && !connected) {
      errorBlock = '<div class="bg-red-50 border border-red-200 rounded-xl p-3 text-xs text-red-600 leading-relaxed">'
        + '<b>⚠️ 连接失败原因：</b><br>' + lastError
        + '<br><br><b>排查建议：</b>'
        + '<br>1. 确认 Supabase 项目状态为 Active（暂停的项目需恢复）'
        + '<br>2. 确认已在 SQL Editor 运行建表语句（dashboard_sync 表）'
        + '<br>3. 确认 URL 和 anon key 填写正确'
        + '<br>4. 国内网络可能需要代理才能访问 Supabase'
        + '</div>';
    }
    // 数据状态显示
    var status = getDataStatus();
    var taskInfo = status['ticktick_pro_state'] || {};
    var diaryInfo = status['dayone_premium_diaries'] || {};
    var assetInfo = status['asset_tracker_state'] || {};
    var healthInfo = status['health_pro_state'] || {};
    var reviewsInfo = status['reviews_pro_state'] || {};
    var trashInfo = status['trash_pro_state'] || {};
    var dataStatusBlock = ''
    + '<div class="bg-slate-50 rounded-xl p-3 space-y-2">'
    +   '<div class="text-xs font-bold text-slate-400 uppercase tracking-wide">本地数据状态</div>'
    +   '<div class="flex items-center justify-between text-sm">'
    +     '<span class="text-slate-600">📋 任务管理</span>'
    +     '<span class="font-semibold ' + (taskInfo.exists && taskInfo.tasks > 0 ? 'text-emerald-600' : 'text-slate-400') + '">' + (taskInfo.summary || '无数据') + '</span>'
    +   '</div>'
    +   '<div class="flex items-center justify-between text-sm">'
    +     '<span class="text-slate-600">📖 闪念日记</span>'
    +     '<span class="font-semibold ' + (diaryInfo.exists && diaryInfo.diaries > 0 ? 'text-emerald-600' : 'text-slate-400') + '">' + (diaryInfo.summary || '无数据') + '</span>'
    +   '</div>'
    +   '<div class="flex items-center justify-between text-sm">'
    +     '<span class="text-slate-600">📦 归物追踪</span>'
    +     '<span class="font-semibold ' + (assetInfo.exists && assetInfo.assets > 0 ? 'text-emerald-600' : 'text-slate-400') + '">' + (assetInfo.summary || '无数据') + '</span>'
    +   '</div>'
    +   '<div class="flex items-center justify-between text-sm">'
    +     '<span class="text-slate-600">💪 健康管理</span>'
    +     '<span class="font-semibold ' + (healthInfo.exists && healthInfo.size > 0 ? 'text-emerald-600' : 'text-slate-400') + '">' + (healthInfo.summary || '无数据') + '</span>'
    +   '</div>'
    +   '<div class="flex items-center justify-between text-sm">'
    +     '<span class="text-slate-600">🍴 小众点评</span>'
    +     '<span class="font-semibold ' + (reviewsInfo.exists && reviewsInfo.reviews > 0 ? 'text-emerald-600' : 'text-slate-400') + '">' + (reviewsInfo.summary || '无数据') + '</span>'
    +   '</div>'
    +   '<div class="flex items-center justify-between text-sm">'
    +     '<span class="text-slate-600">🗑️ 垃圾桶</span>'
    +     '<span class="font-semibold ' + (trashInfo.exists && trashInfo.thoughts > 0 ? 'text-emerald-600' : 'text-slate-400') + '">' + (trashInfo.summary || '无数据') + '</span>'
    +   '</div>'
    + '</div>';
    return ''
    + '<div class="space-y-5">'
    +   '<div class="text-center">'
    +     '<div class="text-xs font-bold text-slate-400 uppercase tracking-wide mb-2">我的同步码</div>'
    +     '<div class="flex items-center justify-center gap-2">'
    +       '<span class="text-3xl font-black tracking-[0.2em] text-indigo-600">' + code + '</span>'
    +       '<button onclick="SyncEngine.copyCode()" class="w-9 h-9 flex items-center justify-center rounded-lg bg-slate-100 hover:bg-slate-200 text-slate-500 transition" title="复制">📋</button>'
    +     '</div>'
    +     '<div class="mt-3 bg-amber-50 border border-amber-100 rounded-xl p-3 text-xs text-amber-700 leading-relaxed text-left">'
    +       '<b>多设备自动同步：</b>只要不同设备填的是<b>同一个 Supabase 配置</b>，就会自动进入同一个房间，无需手动输入同步码。本码由配置自动生成（已迁移旧房间数据）。'
    +     '</div>'
    +   '</div>'
    +   dataStatusBlock
    +   errorBlock
    +   '<div class="flex items-center justify-between bg-slate-50 rounded-xl px-4 py-3">'
    +     '<span class="text-sm text-slate-500">连接状态</span>'
    +     '<span id="modal-sync-status" class="text-sm font-bold ' + statusClass + '">' + statusText + '</span>'
    +   '</div>'
    +   '<div class="grid grid-cols-2 gap-3">'
    +     '<button onclick="SyncEngine.syncNow()" class="py-2.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-sm transition">立即同步</button>'
    +     '<button onclick="SyncEngine.joinRoom()" class="py-2.5 rounded-xl bg-slate-100 hover:bg-slate-200 text-slate-600 font-bold text-sm transition">加入其他房间</button>'
    +   '</div>'
    +   '<button onclick="SyncEngine.forcePushAll().then(function(r){ if(r.ok) { alert(\'✅ 已强制推送 \' + r.keys.length + \' 个模块数据到云端！\\n\' + r.keys.join(\', \')); } else { alert(\'❌ 推送失败: \' + (r.reason||\'未知\')); } SyncEngine.openSyncModal(); })" class="w-full py-2.5 rounded-xl bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-sm transition shadow-lg shadow-emerald-200">⬆️ 强制推送全部数据</button>'
    +   '<details class="text-sm">'
    +     '<summary class="cursor-pointer text-slate-400 font-semibold py-1">高级设置</summary>'
    +     '<div class="mt-2 space-y-2">'
    +       '<button onclick="SyncEngine.diagnose()" class="w-full text-left px-3 py-2 rounded-lg hover:bg-slate-50 text-slate-500 text-xs">🔍 诊断连接问题</button>'
    +       '<button onclick="SyncEngine.resetCode()" class="w-full text-left px-3 py-2 rounded-lg hover:bg-slate-50 text-slate-500 text-xs">重新生成同步码（会创建新房间）</button>'
    +       '<button onclick="SyncEngine.disconnect()" class="w-full text-left px-3 py-2 rounded-lg hover:bg-red-50 text-red-500 text-xs">断开云同步（保留本地数据）</button>'
    +     '</div>'
    +   '</details>'
    + '</div>';
  }

  /* ---------- UI 动作 ---------- */
  function saveAndConnect() {
    var url = (document.getElementById('sb-url').value || '').trim();
    var key = (document.getElementById('sb-key').value || '').trim();
    if (!url || !key) { alert('请填写完整的 URL 和 anon key'); return; }
    if (url.indexOf('supabase.co') === -1) { alert('URL 看起来不像 Supabase 项目地址，请检查'); return; }
    // 自动清理 URL：移除末尾斜杠和 /v1 等多余路径
    url = url.replace(/\/+$/, ''); // 去掉末尾斜杠
    url = url.replace(/\/v1$/, ''); // 去掉末尾 /v1
    // 自动清理 key：移除首尾空白和换行
    key = key.replace(/\s+/g, '');
    saveConfig({ url: url, anonKey: key });
    // 回填清理后的值到输入框（让用户看到）
    document.getElementById('sb-url').value = url;
    document.getElementById('sb-key').value = key;
    // 重置 meta，让本地数据全部上传
    setMeta({});
    sb = null; started = false;
    renderModalBody();
    start().then(function () { pullAll(); forcePushAll(); });
  }

  function copyCode() {
    var code = getCode();
    if (navigator.clipboard) { navigator.clipboard.writeText(code).then(function () { toast('同步码已复制'); }); }
    else { prompt('复制同步码：', code); }
  }

  function joinRoom() {
    var code = prompt('请输入另一台设备的同步码（8位）：', '');
    if (!code) return;
    code = code.trim().toUpperCase();
    if (code.length < 6) { alert('同步码至少 6 位'); return; }
    if (code === getCode()) { alert('这就是当前设备的同步码'); return; }
    if (!confirm('加入新房间会下载该房间的数据覆盖当前本地数据，确定吗？\n（建议先在原设备备份）')) return;
    setCode(code, true);
    setMeta({});
    lastSnapshot = {};
    sb = null; realtimeCh = null; started = false;
    renderModalBody();
    start(true).then(function () { pullAll(); });
    toast('已加入房间 ' + code + '，正在同步…');
  }

  function resetCode() {
    if (!confirm('重新生成同步码将创建一个全新的空房间，当前房间的数据仍保留在云端。确定吗？')) return;
    var newCode = genCode(8);
    setCode(newCode, true);
    setMeta({});
    lastSnapshot = {};
    sb = null; realtimeCh = null; started = false;
    renderModalBody();
    start().then(function () { forcePushAll(); });
  }

  function disconnect() {
    if (!confirm('断开云同步后，本设备恢复纯本地模式（数据不丢失，也不再上传）。确定吗？')) return;
    localStorage.removeItem(CONFIG_KEY);
    sb = null; started = false; connected = false;
    if (pullTimer) clearInterval(pullTimer);
    if (realtimeCh) { try { realtimeCh.unsubscribe(); } catch (e) {} realtimeCh = null; }
    renderModalBody();
    updateStatusBadge();
  }

  function syncNow() {
    toast('正在同步…');
    // 先推送本地数据（确保两个模块都上传），再拉取云端
    forcePushAll().then(function (pushResult) {
      if (pushResult.ok) {
        toast('已推送 ' + (pushResult.keys || []).length + ' 个模块数据，正在拉取…');
      }
      pullAll().then(function (r) {
        notifyIframes();
        if (r && r.changed) { toast('同步完成，数据已更新'); }
        else { toast(pushResult.ok ? '推送完成，数据已是最新' : '同步完成'); }
        renderModalBody();
      });
    });
  }

  function toast(msg) {
    var t = document.createElement('div');
    t.textContent = msg;
    t.style.cssText = 'position:fixed;left:50%;bottom:80px;transform:translateX(-50%);background:rgba(15,23,42,.92);color:#fff;padding:10px 20px;border-radius:12px;font-size:13px;font-weight:600;z-index:200;animation:fade .3s ease;box-shadow:0 8px 24px rgba(0,0,0,.2)';
    document.body.appendChild(t);
    setTimeout(function () { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; }, 1800);
    setTimeout(function () { if (t.parentNode) t.parentNode.removeChild(t); }, 2200);
  }

  /* ---------- 诊断 ---------- */
  async function diagnose() {
    var cfg = loadConfig();
    var results = [];
    // 1. 配置检查
    if (!cfg.url) { results.push('❌ Supabase URL 未填写'); }
    else if (cfg.url.indexOf('supabase.co') === -1) { results.push('⚠️ URL 格式可疑：' + cfg.url); }
    else { results.push('✅ URL: ' + cfg.url); }
    if (!cfg.anonKey) { results.push('❌ anon key 未填写'); }
    else { results.push('✅ anon key 已填写（' + cfg.anonKey.length + ' 字符）'); }
    results.push('✅ 同步码: ' + getCode());

    // 2. 库加载检查
    results.push('');
    results.push('── 库加载 ──');
    if (window.supabase) { results.push('✅ Supabase JS 库已加载'); }
    else {
      results.push('⏳ 正在测试 CDN 连接...');
      try { await loadLib(); results.push('✅ Supabase JS 库加载成功'); }
      catch (e) { results.push('❌ 库加载失败: ' + e.message); }
    }

    // 3. API 连接检查
    if (cfg.url && cfg.anonKey) {
      results.push('');
      results.push('── API 连接 ──');
      // URL 格式检查
      if (cfg.url.indexOf(' ') !== -1 || cfg.url !== cfg.url.trim()) {
        results.push('⚠️ URL 含空格或首尾空白');
      }
      if (cfg.url !== cfg.url.replace(/\/+$/, '')) {
        results.push('⚠️ URL 末尾有斜杠');
      }
      if (/\/v1$/.test(cfg.url)) {
        results.push('⚠️ URL 末尾含有 /v1（应删除）');
      }
      try {
        var client = await ensureClient();
        if (client) {
          results.push('✅ Supabase 客户端创建成功');
          var res = await client.from('dashboard_sync').select('count').eq('sync_code', getCode()).limit(1);
          if (res.error) {
            // 专门识别 "Invalid path" 错误
            if (res.error.message.indexOf('Invalid path') !== -1 || res.error.message.indexOf('InvalidPath') !== -1) {
              results.push('❌ URL 格式错误：' + res.error.message);
              results.push('   → 正确格式：https://xxxxx.supabase.co');
              results.push('   → 当前格式：' + cfg.url);
              results.push('   → 末尾的 / 或 /v1 必须删除');
              results.push('   → 修复方法：点击「断开云同步」重新填写');
            } else {
              results.push('❌ 查询失败: ' + res.error.message);
            }
            if (res.error.message.indexOf('relation') !== -1) {
              results.push('   → 表 dashboard_sync 不存在，请先在 SQL Editor 运行建表语句');
            }
            if (res.error.message.indexOf('permission') !== -1 || res.error.message.indexOf('policy') !== -1) {
              results.push('   → RLS 策略问题，请确认已运行 CREATE POLICY 语句');
            }
          } else {
            results.push('✅ 数据库查询成功 — 表和权限正常');
          }
        } else {
          results.push('❌ 客户端创建失败');
        }
      } catch (e) {
        results.push('❌ 连接异常: ' + (e.message || String(e)));
      }
    }

    // 4. Realtime 检查
    results.push('');
    results.push('── 实时订阅 ──');
    if (realtimeCh) { results.push('✅ Realtime 频道已创建'); }
    else { results.push('⚠️ Realtime 频道未创建（连接成功后会自动创建）'); }

    // 5. 总结
    results.push('');
    results.push('── 总结 ──');
    if (connected) { results.push('✅ 同步正常运行中'); }
    else if (lastError) { results.push('❌ 连接异常: ' + lastError); }
    else { results.push('⏳ 正在连接中...'); }

    alert(results.join('\n'));
  }

  /* ---------- 数据状态报告（诊断用）---------- */
  function getDataStatus() {
    var snap = snapshotLocal();
    var status = {};
    SYNC_KEYS.forEach(function (k) {
      var val = snap[k];
      var info = { key: k, exists: !!val, size: val ? val.length : 0 };
      if (val) {
        try {
          var parsed = JSON.parse(val);
          if (k === 'ticktick_pro_state') {
            info.tasks = (parsed.tasks || []).length;
            info.habits = (parsed.habits || []).length;
            info.countdowns = (parsed.countdowns || []).length;
            info.lists = (parsed.lists || []).length;
            info.summary = parsed.tasks ? (parsed.tasks.length + ' 个任务, ' + (parsed.habits||[]).length + ' 个习惯') : '空';
          } else if (k === 'dayone_premium_diaries') {
            info.diaries = Array.isArray(parsed) ? parsed.length : 0;
            info.summary = info.diaries + ' 篇日记';
          } else if (k === 'asset_tracker_state') {
            info.assets = Array.isArray(parsed.assets) ? parsed.assets.length : 0;
            info.categories = Array.isArray(parsed.categories) ? parsed.categories.length : 0;
            info.summary = info.assets + ' 个物品, ' + info.categories + ' 个分类';
          } else if (k === 'health_pro_state') {
            info.summary = '已记录';
          } else if (k === 'reviews_pro_state') {
            info.reviews = Array.isArray(parsed.reviews) ? parsed.reviews.length : 0;
            info.summary = info.reviews + ' 条点评';
          } else if (k === 'trash_pro_state') {
            info.thoughts = (parsed.thoughts || []).length;
            info.summary = info.thoughts + ' 个念头';
          }
        } catch (e) {
          info.summary = '解析失败';
        }
      } else {
        info.summary = '无数据';
      }
      status[k] = info;
    });
    return status;
  }

  /* ---------- 暴露 API ---------- */
  window.SyncEngine = {
    isConfigured: isConfigured,
    getCode: getCode,
    start: start,
    pullNow: pullAll,
    pushNow: doPush,
    forcePushAll: forcePushAll,
    getDataStatus: getDataStatus,
    openSyncModal: openSyncModal,
    closeSyncModal: closeSyncModal,
    saveAndConnect: saveAndConnect,
    copyCode: copyCode,
    joinRoom: joinRoom,
    resetCode: resetCode,
    disconnect: disconnect,
    syncNow: syncNow,
    diagnose: diagnose,
    refreshIframe: refreshIframe
  };

  // 页面加载后若已配置则自动启动
  window.addEventListener('load', function () {
    // 启动时自动清理历史配置中的 URL 格式问题
    var cfg = loadConfig();
    if (cfg.url) {
      var cleaned = cfg.url.replace(/\/+$/, '').replace(/\/v1$/, '');
      if (cleaned !== cfg.url) {
        cfg.url = cleaned;
        saveConfig(cfg);
      }
    }
    if (isConfigured()) start();
    updateStatusBadge();
  });
})();
