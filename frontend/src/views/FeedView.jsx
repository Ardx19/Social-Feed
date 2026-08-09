import { useState } from 'react'
import { api, errorText } from '../App.jsx'

// Feed View: enter a user ID, see the merged feed with load-more pagination.
export default function FeedView() {
  const [userId, setUserId] = useState('')
  const [entries, setEntries] = useState([])
  const [cursor, setCursor] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [note, setNote] = useState(null)

  async function load(first) {
    if (!userId.trim()) {
      setError('Enter a user ID')
      return
    }
    setLoading(true)
    setError(null)
    try {
      const before = first ? '' : cursor || ''
      const body = await api(`/feed/${encodeURIComponent(userId.trim())}?limit=20&before=${before}`)
      // Reset the list on a fresh load; append when paging.
      setEntries(first ? body.entries : [...entries, ...body.entries])
      setCursor(body.next_cursor || null)
      if (body.entries.length === 0 && !first) setNote('No more posts')
      else setNote(null)
    } catch (e) {
      setError(errorText(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="view">
      <h2>Feed</h2>
      <div className="row">
        <input
          placeholder="user ID, e.g. alice"
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && load(true)}
        />
        <button disabled={loading || !userId.trim()} onClick={() => load(true)}>
          {loading ? 'Loading…' : 'Show feed'}
        </button>
      </div>
      {error && <div className="error">{error}</div>}
      {note && <div className="note">{note}</div>}

      {entries.length > 0 && (
        <ul className="feed">
          {entries.map((e) => (
            <li key={e.post_id} className="feed-item">
              <div className="feed-author">{e.author_id}</div>
              <div className="feed-content">{e.content}</div>
              <div className="feed-meta">#{e.post_id} · {new Date(e.timestamp / 1e6).toLocaleString()}</div>
            </li>
          ))}
        </ul>
      )}
      {!loading && !error && entries.length === 0 && !note && (
        <div className="note">No posts yet</div>
      )}
      {cursor && (
        <button disabled={loading} onClick={() => load(false)}>
          {loading ? 'Loading…' : 'Load more'}
        </button>
      )}
    </div>
  )
}
