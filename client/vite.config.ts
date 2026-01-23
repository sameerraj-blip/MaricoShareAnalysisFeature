import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  define: {
    global: 'globalThis',
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
      "@shared": path.resolve(__dirname, "src/shared"),
      "@assets": path.resolve(__dirname, "../attached_assets"),
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': 'http://localhost:3002'
    }
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        manualChunks: {
          // Vendor chunks
          'react-vendor': ['react', 'react-dom', 'react-router'],
          'ui-vendor': [
            '@radix-ui/react-dialog',
            '@radix-ui/react-dropdown-menu',
            '@radix-ui/react-select',
            '@radix-ui/react-tooltip',
            '@radix-ui/react-toast',
          ],
          'chart-vendor': ['recharts'],
          'query-vendor': ['@tanstack/react-query'],
          'msal-vendor': ['@azure/msal-browser', '@azure/msal-react'],
          'utils-vendor': ['axios', 'date-fns', 'zod'],
          'grid-vendor': ['react-grid-layout', 'react-resizable'],
        },
        // Optimize chunk size
        chunkSizeWarningLimit: 1000,
      },
    },
    // Enable source maps for better debugging in production (optional)
    sourcemap: false,
    // Optimize chunk splitting
    chunkSizeWarningLimit: 1000,
  },
  // Optimize dependencies
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      '@tanstack/react-query',
      'recharts',
      '@azure/msal-browser',
      '@azure/msal-react',
    ],
  },
});