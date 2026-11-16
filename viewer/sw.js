// Service worker for the Momentum Rank Flow viewer.
// Strategy: network-first for everything we handle, falling back to the last
// cached copy when offline. The viewer is edited often and the data changes
// daily, so a stale-first strategy would keep showing old content.
const CACHE = 'rank-flow-v1';          // bump to drop old caches after a shell change
const SHELL = [
  './',
  './index.html',
  './manifest.webmanifest',
  './icons/icon-192.png',
  './icons/icon-512.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  const isData = url.hostname === 'raw.githubusercontent.com';
  const isOwn = url.origin === self.location.origin;
  if (!isData && !isOwn) return;       // fonts etc. go straight to the network

  // The page adds ?t=<timestamp> to data requests to defeat HTTP caching;
  // key the cache on the URL without the query so entries don't pile up.
  const key = isData ? url.origin + url.pathname : req.url;

  event.respondWith(
    fetch(req)
      .then((res) => {
        if (res && res.ok) {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(key, copy));
        }
        return res;
      })
      .catch(() =>
        caches.match(key).then((hit) => {
          if (hit) return hit;
          if (req.mode === 'navigate') return caches.match('./index.html');
          return Response.error();
        })
      )
  );
});
