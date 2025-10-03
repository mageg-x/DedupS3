import { createI18n } from 'vue-i18n';
import messages from './messages';

// 创建i18n实例
const i18n = createI18n({
  legacy: false,
  locale: localStorage.getItem('language') || 'zh', // 默认中文
  fallbackLocale: 'zh',
  messages
});

export default i18n;