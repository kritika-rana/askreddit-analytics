import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    plugins: [react()],
    server: {
        port: 3011,
        host: true,
        proxy: {
            '/api': 'http://10.0.0.38:3010'
        }
    }
})