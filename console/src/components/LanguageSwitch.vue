<template>
  <div class="language-switch relative">
    <button 
      @click="toggleDropdown"
      class="flex items-center gap-2 p-2 text-gray-600 hover:text-blue-500 transition-all duration-300 rounded-full hover:bg-gray-100"
    >
      <i class="fas fa-globe text-lg"></i>
      <span v-if="!sidebarCollapsed" class="text-sm font-medium">{{ currentLanguageName }}</span>
    </button>
    
    <div 
      v-if="isDropdownOpen"
      class="absolute right-0 mt-2 w-40 bg-white rounded-lg shadow-lg overflow-hidden z-50"
    >
      <button 
        v-for="lang in languages" 
        :key="lang.code"
        @click="switchLanguage(lang.code)"
        class="w-full text-left px-4 py-2 text-sm flex items-center gap-2 hover:bg-gray-100 transition-colors"
        :class="{ 'bg-blue-50 text-blue-600': currentLanguage === lang.code }"
      >
        <i :class="lang.icon" class="w-5 h-5"></i>
        <span>{{ lang.name }}</span>
        <i v-if="currentLanguage === lang.code" class="fas fa-check ml-auto text-xs"></i>
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElMessage } from 'element-plus';

const props = defineProps({
  sidebarCollapsed: Boolean
});

const { locale } = useI18n();
const isDropdownOpen = ref(false);
const currentLanguage = ref(locale.value);

const languages = [
  { code: 'zh', name: '简体中文', icon: 'fas fa-language' },
  { code: 'en', name: 'English', icon: 'fas fa-language' }
];

const currentLanguageName = computed(() => {
  const currentLang = languages.find(lang => lang.code === currentLanguage.value);
  return currentLang ? currentLang.name : '';
});

const toggleDropdown = () => {
  isDropdownOpen.value = !isDropdownOpen.value;
};

const switchLanguage = (langCode) => {
  if (currentLanguage.value === langCode) {
    isDropdownOpen.value = false;
    return;
  }
  
  currentLanguage.value = langCode;
  locale.value = langCode;
  localStorage.setItem('language', langCode);
  
  // 通知用户语言已切换
  ElMessage.success({
    message: langCode === 'zh' ? '语言已切换为简体中文' : 'Language switched to English',
    duration: 2000
  });
  
  // 切换语言后刷新页面，确保所有组件都应用新语言
  setTimeout(() => {
    window.location.reload();
  }, 1000);
  
  isDropdownOpen.value = false;
};

// 点击其他地方关闭下拉菜单
const handleClickOutside = (event) => {
  const dropdown = document.querySelector('.language-switch');
  if (dropdown && !dropdown.contains(event.target)) {
    isDropdownOpen.value = false;
  }
};

// 监听点击事件
watch(() => isDropdownOpen.value, (newValue) => {
  if (newValue) {
    document.addEventListener('click', handleClickOutside);
  } else {
    document.removeEventListener('click', handleClickOutside);
  }
});
</script>

<style scoped>
.language-switch {
  position: relative;
}

.language-switch button {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.language-switch button:hover {
  transform: translateY(-1px);
}

.language-switch button:active {
  transform: translateY(0);
}
</style>