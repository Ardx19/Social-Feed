import { useState } from 'react'
import FeedView from './views/FeedView.jsx'
import PostView from './views/PostView.jsx'
import FollowView from './views/FollowView.jsx'

// apiBase is the coordinator. In dev, Vite proxies /api -> localhost:8080 and
// rewrites the prefix away; pointing at /api keeps the production build able to
// sit behind the same proxy. See vite.config.js.
export const apiBase = '/api'

// A tiny fetch wrapper that surfaces the HTTP status so views can render
// distinct messages per code (400 validation, 404 not found, 502 degraded,
// 503 busy) instead of one generic outage message.
export async function api(path, options = {}) {
  const res = await fetch(apiBase + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  let body = null
  try {
    body = await res.json()
  } catch {
    // non-JSON body
  }
  if (!res.ok) {
    const msg = (body && body.error) || `HTTP ${res.status}`
    throw { status: res.status, message: msg }
  }
  return body
}

export function errorText(err) {
  if (err && err.status === 400) return 'Bad request: ' + err.message
  if (err && err.status === 404) return 'Not found: ' + err.message
  if (err && err.status === 502) return 'Feed unavailable, try again'
  if (err && err.status === 503) return 'System busy, try again'
  return 'Connection to the coordinator failed'
}

const TABS = [
  { key: 'feed', label: 'Feed' },
  { key: 'post', label: 'Post' },
  { key: 'follow', label: 'Follow' },
]

export default function App() {
  const [tab, setTab] = useState('feed')
  return (
    <div className="app">
      <header className="app-header">
        <h1>Distributed Social Feed</h1>
        <nav className="tabs">
          {TABS.map((t) => (
            <button
              key={t.key}
              className={'tab' + (tab === t.key ? ' active' : '')}
              onClick={() => setTab(t.key)}
            >
              {t.label}
            </button>
          ))}
        </nav>
      </header>
      <main className="content">
        {tab === 'feed' && <FeedView />}
        {tab === 'post' && <PostView />}
        {tab === 'follow' && <FollowView />}
      </main>
    </div>
  )
}
