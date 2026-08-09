import { useState } from 'react'
import { api, errorText } from '../App.jsx'

// Post View: author ID + content, POST /posts, show confirmation.
export default function PostView() {
  const [authorId, setAuthorId] = useState('')
  const [content, setContent] = useState('')
  const [sending, setSending] = useState(false)
  const [error, setError] = useState(null)
  const [done, setDone] = useState(null)

  async function submit() {
    if (!authorId.trim() || !content.trim()) {
      setError('Author ID and content are required')
      return
    }
    setSending(true)
    setError(null)
    setDone(null)
    try {
      const body = await api('/posts', {
        method: 'POST',
        body: JSON.stringify({ author_id: authorId.trim(), content: content.trim() }),
      })
      setDone(`Posted as ${body.post_id}`)
      setContent('')
    } catch (e) {
      setError(errorText(e))
    } finally {
      setSending(false)
    }
  }

  return (
    <div className="view">
      <h2>Post</h2>
      <div className="field">
        <label>Author ID</label>
        <input
          placeholder="e.g. alice"
          value={authorId}
          onChange={(e) => setAuthorId(e.target.value)}
        />
      </div>
      <div className="field">
        <label>Content</label>
        <textarea
          rows={4}
          value={content}
          onChange={(e) => setContent(e.target.value)}
        />
      </div>
      {error && <div className="error">{error}</div>}
      {done && <div className="success">{done}</div>}
      <button disabled={sending || !authorId.trim() || !content.trim()} onClick={submit}>
        {sending ? 'Posting…' : 'Submit post'}
      </button>
    </div>
  )
}
