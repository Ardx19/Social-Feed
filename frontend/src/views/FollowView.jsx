import { useState } from 'react'
import { api, errorText } from '../App.jsx'

// Follow View: follow/unfollow one user at a time, and show the current
// following list. The user's own self-edge is filtered out of the rendered
// list (it is a system invariant, not a user-managed follow — see design.md).
export default function FollowView() {
  const [viewer, setViewer] = useState('')
  const [follower, setFollower] = useState('')
  const [followee, setFollowee] = useState('')
  const [following, setFollowing] = useState([])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  const [note, setNote] = useState(null)

  async function refresh() {
    if (!viewer.trim()) {
      setError('Enter a user ID to see who they follow')
      return
    }
    setBusy(true)
    setError(null)
    try {
      const body = await api(`/users/${encodeURIComponent(viewer.trim())}/following`)
      setFollowing(body.following || [])
      setNote(null)
    } catch (e) {
      setError(errorText(e))
    } finally {
      setBusy(false)
    }
  }

  async function act(unfollow) {
    if (!follower.trim() || !followee.trim()) {
      setError('Follower and followee are required')
      return
    }
    setBusy(true)
    setError(null)
    setNote(null)
    try {
      const path = unfollow ? '/unfollow' : '/follow'
      await api(path, {
        method: 'POST',
        body: JSON.stringify({ follower: follower.trim(), followee: followee.trim() }),
      })
      setNote(unfollow ? `Unfollowed ${followee}` : `Now following ${followee}`)
      // Refresh the follow list so it reflects real backend state.
      await refreshList()
    } catch (e) {
      setError(errorText(e))
    } finally {
      setBusy(false)
    }
  }

  async function refreshList() {
    if (!viewer.trim()) return
    try {
      const body = await api(`/users/${encodeURIComponent(viewer.trim())}/following`)
      setFollowing(body.following || [])
    } catch {
      // keep prior list on refresh failure
    }
  }

  const filtered = following

  return (
    <div className="view">
      <h2>Follow</h2>
      <div className="field">
        <label>Viewer (who "I" am)</label>
        <input placeholder="e.g. alice" value={viewer} onChange={(e) => setViewer(e.target.value)} />
      </div>
      <div className="row">
        <input placeholder="follower" value={follower} onChange={(e) => setFollower(e.target.value)} />
        <input placeholder="followee" value={followee} onChange={(e) => setFollowee(e.target.value)} />
        <button disabled={busy} onClick={() => act(false)}>{busy ? '…' : 'Follow'}</button>
        <button disabled={busy} onClick={() => act(true)}>{busy ? '…' : 'Unfollow'}</button>
      </div>
      {error && <div className="error">{error}</div>}
      {note && <div className="note">{note}</div>}
      {filtered.length > 0 && (
        <div className="following">
          <strong>Following ({filtered.length}):</strong> {filtered.join(', ')}
        </div>
      )}
    </div>
  )
}
