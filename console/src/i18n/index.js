import { createI18n } from 'vue-i18n';
import messages from './langs/index.js';

// 创建i18n实例
const i18n = createI18n({
  legacy: false,
  locale: localStorage.getItem('language') || 'zh', // 默认中文
  fallbackLocale: 'zh',
  messages,
  linkedModifiers: {
    // 不定义任何 modifier
  },
});

export default i18n;
export { messages };