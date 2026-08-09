import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev proxy: forward API calls to the coordinator on localhost:8080.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        // rewrite: the backend routes are at the root (/users, /feed/...),
        // so strip the /api prefix for the coordinator but keep it for the UI.
        // Simpler: the frontend calls the coordinator directly below; keep a
        // passthrough here as a convenience.
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
