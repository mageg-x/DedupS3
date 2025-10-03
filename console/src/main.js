import { createApp } from 'vue'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import './style.css'
import App from './App.vue'
import router from './router'
import * as ElementPlusIconsVue from '@element-plus/icons-vue';
import i18n from './i18n';
// 设置Element Plus的国际化
import zhCn from 'element-plus/es/locale/lang/zh-cn'
import en from 'element-plus/es/locale/lang/en'

const app = createApp(App)

// 全局注册所有图标组件
for (const [name, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(name, component);
}

// 根据当前语言设置Element Plus的语言
const locale = localStorage.getItem('language') || 'zh';
app.use(ElementPlus, {
  locale: locale === 'zh' ? zhCn : en
})
app.use(router)
app.use(i18n)
app.mount('#app')
