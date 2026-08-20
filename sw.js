// Service Worker for 个人看板 PWA - offline cache
const CACHE = 'dashboard-v97';
const ASSETS = [
  './',
  './index.html',
  './app-tasks.html',
  './app-diary.html',
  './app-assets.html',
  './app-reviews.html',
  './app-health.html',
  './app-trash.html',
  './sync.js',
  './lunar.js',
  './tailwind.css',
  './manifest.json',
  './icon-192.png',
  './icon-512.png',
  './icon-180.png'
];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)).catch(() => {}));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))));
  self.clients.claim();
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  // Network-first for HTML, sync.js, lunar.js, and navigation (ensures updates take effect)
  if (req.mode === 'navigate' || url.pathname.endsWith('.html') || url.pathname.endsWith('sync.js') || url.pathname.endsWith('lunar.js')) {
    e.respondWith(fetch(req).then(res => {
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(req, copy)).catch(() => {});
      return res;
    }).catch(() => caches.match(req).then(r => r || caches.match('./index.html'))));
  } else {
    // Cache-first for static assets (CSS, images)
    e.respondWith(caches.match(req).then(r => r || fetch(req).then(res => {
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(req, copy)).catch(() => {});
      return res;
    }).catch(() => r)));
  }
});
