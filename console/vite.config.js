import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import tailwindcss from "@tailwindcss/vite";
import path from 'path';


// https://vite.dev/config/
export default defineConfig({
  root: "./src",
  plugins: [vue(), tailwindcss()],
  server: {
    proxy: {
      '/api': {
        target: 'http://172.17.54.243:3002',
        changeOrigin: true,
        secure: false,
        // 👇 只代理非静态资源的请求
        bypass: (req) => {
          // 如果是静态资源，就不代理，让 Vite 自己处理
          if (req.url.match(/\.(js|css|png|jpg|jpeg|gif|ico|svg)$/)) {
            return req.url;
          }
          // 否则继续代理
        },
        configure: (proxy, options) => {
          proxy.on('proxyRes', (proxyRes, req, res) => {
            // 修改响应头，重写 Set-Cookie 的 Domain
            const cookies = proxyRes.headers['set-cookie'];
            if (cookies && Array.isArray(cookies)) {
              proxyRes.headers['set-cookie'] = cookies.map(cookie =>
                cookie.replace(/Domain=[^;\s]*/i, 'Domain=localhost')
              );
            }
          });
        },
      },
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  build: {
    outDir: "../dist",
    emptyOutDir: true,

    // 按类型分目录
    rollupOptions: {
      output: {
        // JS 文件输出路径（用于入口和 chunk）
        entryFileNames: "js/[name]-[hash].js",
        // 所有模块按包名分目录存放
        chunkFileNames: (chunkInfo) => {
          const name = chunkInfo.name.split("/")[0].replace(/[@]/g, "");
          if (["primevue", "primeuix"].includes(name))
            return `js/primevue/[hash].js`;
          if (["vue", "vue-router"].includes(name)) return `js/vue/[hash].js`;

          // 非 node_modules 模块放在 common 目录
          return "js/vendor/[hash].js";
        },

        // 统一使用 assetFileNames 来按类型分类
        assetFileNames: (assetInfo) => {
          const fileName = assetInfo.names?.[0] || assetInfo.name;
          // 获取扩展名
          const ext = fileName.split(".").pop().toLowerCase();

          // 图片
          if (
            ["png", "jpg", "jpeg", "gif", "svg", "webp", "ico"].includes(ext)
          ) {
            return "img/[ext]/[name]-[hash][extname]";
          }

          // 字体
          if (["woff", "woff2", "eot", "ttf", "otf"].includes(ext)) {
            return "fonts/[ext]/[name]-[hash][extname]";
          }

          // CSS 文件
          if (ext === "css") {
            return "css/[name]-[hash][extname]";
          }

          // 其他资源（如 webmanifest）
          return "[name]-[hash][extname]";
        },

        // 手动分块策略
        manualChunks(id) {
          if (id.includes("node_modules")) {
            // 提取完整的包名 (包含 scope)
            const match = id.match(
              /[\\/]node_modules[\\/](@[^\\/]+\/[^\\/]+|[^\\/]+)/
            );
            if (match) {
              return match[1]; // 返回如 "vue"、"@vue/router"、"primevue/button"
            }

            // 无法识别的模块放在 vendor
            return "vendor";
          }
        },
      },
    },
  },
});
