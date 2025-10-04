<template>
  <div class="main-layout min-h-screen bg-gray-50 text-gray-900">
    <!-- 侧边栏 -->
    <aside
      :class="['sidebar bg-white shadow-md transition-all duration-300 ease-in-out fixed top-0 left-0 z-30 h-full', sidebarCollapsed ? 'w-20' : 'w-72']">
      <!-- 品牌Logo -->
      <div :class="['sidebar-header p-5 flex items-center justify-between', sidebarCollapsed ? 'justify-center' : '']">
        <div v-if="!sidebarCollapsed" class="flex items-center gap-3 animate-fadeIn">
          <div
            class="w-10 h-10 bg-gradient-to-br from-blue-500 to-purple-600 rounded-lg flex items-center justify-center shadow-md">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"
              class="w-6 h-6 text-white">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
            </svg>
          </div>
          <span class="text-xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 text-transparent bg-clip-text">{{
            t('brand.name') }}</span>
        </div>
        <div v-else
          class="w-10 h-10 bg-gradient-to-br from-blue-500 to-purple-600 rounded-lg flex items-center justify-center shadow-md">
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor"
            class="w-6 h-6 text-white">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
          </svg>
        </div>
        <!-- 折叠按钮 -->
        <button v-if="!sidebarCollapsed" @click="toggleSidebar"
          class="text-gray-500 hover:text-blue-500 transition-all duration-300 p-2 rounded-full hover:bg-gray-100">
          <i class="fas fa-angle-left text-lg"></i>
        </button>
        <button v-else @click="toggleSidebar"
          class="absolute right-[-12px] top-1/2 transform -translate-y-1/2 w-6 h-12 bg-white rounded-r-lg shadow-md flex items-center justify-center text-gray-500 hover:text-blue-500 transition-all duration-300">
          <i class="fas fa-angle-right text-lg"></i>
        </button>
      </div>

      <!-- 菜单列表 -->
      <nav class="py-4">
        <ul>
          <li v-for="(item, index) in menuItems" :key="item.path || `menu-${index}`"
            :class="{ 'has-children': item.children && item.children.length > 0 }">
            <!-- 有子菜单的菜单项 -->
            <div v-if="item.children && item.children.length > 0" :class="['relative']">
              <button @click="toggleSubmenu(index)"
                :class="['flex items-center p-3.5 transition-all duration-300 w-full text-left', isActiveMenuItem(item) ? 'bg-blue-50 text-blue-600 border-r-4 border-blue-500 shadow-sm' : 'hover:bg-gray-50 hover:text-blue-500']"
                :style="{ animationDelay: index * 0.05 + 's' }">
                <div class="flex items-center justify-center w-10 h-10 rounded-lg transition-all duration-300"
                  :class="isActiveMenuItem(item) ? 'bg-blue-100' : 'hover:bg-gray-100'">
                  <i v-if="item.icon !== 'svg-bucket'" :class="['fas', item.icon, 'w-6 h-6']"></i>
                  <svg v-else class="w-4 h-4" viewBox="0 0 256 256" fill="currentColor"
                    xmlns="http://www.w3.org/2000/svg">
                    <g>
                      <path
                        d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z" />
                    </g>
                  </svg>
                </div>
                <span v-if="!sidebarCollapsed" class="ml-3 text-sm font-medium">{{ item.label }}</span>
                <i v-if="!sidebarCollapsed"
                  :class="['fas absolute right-3 text-gray-400 transition-transform duration-300', isSubmenuOpen[index] ? 'fa-chevron-down rotate-180' : 'fa-chevron-down']"></i>
              </button>
              <!-- 子菜单 -->
              <ul v-if="isSubmenuOpen[index] && !sidebarCollapsed" class="pl-12 py-1 bg-white animate-fadeIn">
                <li v-for="subItem in item.children" :key="subItem.path">
                  <router-link :to="subItem.path"
                    :class="['flex items-center py-3.5 transition-all duration-300', isActiveRoute(subItem.path) ? 'bg-blue-50 text-blue-600' : 'hover:bg-gray-50 hover:text-blue-500']">
                    <div class="flex items-center justify-center w-8 h-8 rounded-lg transition-all duration-300"
                      :class="isActiveRoute(subItem.path) ? 'bg-blue-100' : 'hover:bg-gray-100'">
                      <i v-if="subItem.icon !== 'svg-bucket'" :class="['fas', subItem.icon, 'w-5 h-5']"></i>
                      <svg v-else class="w-3 h-3" viewBox="0 0 256 256" fill="currentColor"
                        xmlns="http://www.w3.org/2000/svg">
                        <g>
                          <path
                            d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z" />
                        </g>
                      </svg>
                    </div>
                    <span class="ml-2 text-sm font-medium">{{ subItem.label }}</span>
                  </router-link>
                </li>
              </ul>
            </div>
            <!-- 没有子菜单的菜单项 -->
            <router-link v-else :to="item.path"
              :class="['flex items-center p-3.5 transition-all duration-300', isActiveRoute(item.path) ? 'bg-blue-50 text-blue-600 border-r-4 border-blue-500 shadow-sm' : 'hover:bg-gray-50 hover:text-blue-500']"
              :style="{ animationDelay: index * 0.05 + 's' }">
              <div class="flex items-center justify-center w-10 h-10 rounded-lg transition-all duration-300"
                :class="isActiveRoute(item.path) ? 'bg-blue-100' : 'hover:bg-gray-100'">
                <i v-if="item.icon !== 'svg-bucket'" :class="['fas', item.icon, 'w-6 h-6']"></i>
                <svg v-else class="w-4 h-4" viewBox="0 0 256 256" fill="currentColor"
                  xmlns="http://www.w3.org/2000/svg">
                  <g>
                    <path
                      d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z" />
                  </g>
                </svg>
              </div>
              <span v-if="!sidebarCollapsed" class="ml-3 text-sm font-medium">{{ item.label }}</span>
            </router-link>
          </li>
        </ul>
      </nav>
    </aside>

    <!-- 主内容区域 -->
    <div :class="['main-content transition-all duration-300', sidebarCollapsed ? 'ml-20' : 'ml-72']">
      <!-- 顶部导航栏 -->
      <header class="bg-white shadow-sm sticky top-0 z-20">
        <div class="flex items-center justify-between p-5">
          <!-- 面包屑导航 -->
          <div class="hidden md:block">
            <el-breadcrumb separator="/">
              <el-breadcrumb-item v-for="(crumb, index) in breadcrumbs" :key="index">
                <span v-if="index === breadcrumbs.length - 1" class="text-gray-900 font-medium">{{ crumb.label }}</span>
                <router-link v-else :to="crumb.path" class="text-gray-500 hover:text-blue-500 transition-colors">{{
                  crumb.label }}</router-link>
              </el-breadcrumb-item>
            </el-breadcrumb>
          </div>

          <!-- 右侧工具栏 -->
          <div class="flex items-center gap-5">
            <!-- 通知图标 -->
            <button
              class="relative p-2 text-gray-600 hover:text-blue-500 transition-all duration-300 rounded-full hover:bg-gray-100">
              <i class="fas fa-bell text-lg"></i>
              <span class="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full"></span>
            </button>

            <!-- 语言切换 -->
            <LanguageSwitch :sidebar-collapsed="sidebarCollapsed" />

            <!-- 用户信息 -->
            <div class="flex items-center gap-3 cursor-pointer group relative">
              <div class="relative">
                <img v-if="currentUser.avatar" :src="currentUser.avatar" alt="用户头像"
                  class="w-10 h-10 rounded-full border-2 border-transparent hover:border-blue-500 transition-all duration-300 shadow-sm">
                <div v-else
                  class="w-10 h-10 rounded-full bg-gradient-to-br from-blue-100 to-purple-100 flex items-center justify-center text-gray-700 font-medium shadow-sm">
                  {{ currentUser.name ? currentUser.name.charAt(0) : '?' }}
                </div>
                <div
                  class="absolute inset-0 rounded-full bg-blue-500 opacity-0 group-hover:opacity-10 transition-opacity duration-300">
                </div>
              </div>
              <div v-if="!sidebarCollapsed" class="hidden md:block">
                <div class="text-sm font-medium text-gray-800 group-hover:text-blue-600 transition-colors">
                  {{ currentUser.name || '未登录' }}
                </div>
                <div class="text-xs text-gray-500">
                  {{ currentUser.role === 'admin' ? t('brand.admin') : t('brand.regularUser') }}
                </div>
              </div>
              <i
                class="fas fa-chevron-down text-gray-400 hidden md:block group-hover:text-blue-500 transition-colors"></i>
            </div>

            <!-- 退出登录 -->
            <button @click="handleLogout"
              class="p-2 text-gray-600 hover:text-red-500 transition-all duration-300 rounded-full hover:bg-gray-100">
              <i class="fas fa-sign-out-alt text-lg"></i>
            </button>
          </div>
        </div>
      </header>

      <!-- 内容区域 -->
      <main class="p-6">
        <router-view />
      </main>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import { useRoute } from 'vue-router';
import { useI18n } from 'vue-i18n';
import LanguageSwitch from '@/components/LanguageSwitch.vue';

const { t } = useI18n();

const route = useRoute();

// 侧边栏状态
const sidebarCollapsed = ref(false);

// 当前用户信息 - 添加默认mock数据
const currentUser = ref({
  name: '管理员',
  role: 'admin',
  avatar: null
});

// 菜单列表
const menuItems = [
  { path: '/dashboard', label: t('mainMenu.dashboard'), icon: 'fa-home' },
  { path: '/buckets', label: t('mainMenu.buckets'), icon: 'svg-bucket' },
  { path: '/accesskey', label: t('mainMenu.accessKey'), icon: 'fa-key' },
  {
    label: t('mainMenu.iam'),
    icon: 'fa-lock',
    children: [
      { path: '/user', label: t('mainMenu.user'), icon: 'fa-user' },
      { path: '/group', label: t('mainMenu.group'), icon: 'fa-user-group' },
      { path: '/role', label: t('mainMenu.role'), icon: 'fa-user-tag' },
      { path: '/policy', label: t('mainMenu.policy'), icon: 'fa-file-signature' },
    ]
  },
  { path: '/event', label: t('mainMenu.event'), icon: 'fa-bell' },
  { path: '/audit', label: t('mainMenu.audit'), icon: 'fa-file-text' },
  {
    label: t('mainMenu.configuration'),
    icon: 'fa-cog',
    children: [
      { path: '/endpoint', label: t('mainMenu.endpoint'), icon: 'fa-map-marker-alt' },
      { path: '/quota', label: t('mainMenu.quota'), icon: 'fa-tachometer-alt' },
      { path: '/chunk', label: t('mainMenu.chunk'), icon: 'fa-chart-pie' },
    ]
  },
  {
    label: t('mainMenu.advancedFeatures'),
    icon: 'fa-tools',
    children: [
      { path: '/migration', label: t('mainMenu.migration'), icon: 'fa-exchange-alt' },
      { path: '/defragment', label: t('mainMenu.defragment'), icon: 'fa-puzzle-piece' },
      { path: '/snapshot', label: t('mainMenu.snapshot'), icon: 'fa-images' },
      { path: '/analysis', label: t('mainMenu.analysis'), icon: 'fa-kit-medical' },
      { path: '/debugtool', label: t('mainMenu.debugTool'), icon: 'fa-bug' },
    ]
  },
];

// 切换侧边栏
const toggleSidebar = () => {
  sidebarCollapsed.value = !sidebarCollapsed.value;
  // 折叠时关闭所有子菜单
  if (sidebarCollapsed.value) {
    isSubmenuOpen.value.forEach((_, index) => {
      isSubmenuOpen.value[index] = false;
    });
  }
};

// 子菜单展开状态
const isSubmenuOpen = ref([]);

// 切换子菜单
const toggleSubmenu = (index) => {
  isSubmenuOpen.value[index] = !isSubmenuOpen.value[index];
};

// 计算当前激活的路由
const isActiveRoute = (path) => {
  return route.path === path;
};

// 检查菜单项是否包含活动路由（用于有子菜单的菜单项）
const isActiveMenuItem = (item) => {
  if (!item.children) return false;
  return item.children.some(subItem => isActiveRoute(subItem.path));
};

// 计算面包屑导航
const breadcrumbs = computed(() => {
  const path = route.path;
  const crumbs = [{ path: '/dashboard', label: t('common.home') }];

  // 查找当前路径对应的菜单项，包括子菜单
  let found = false;
  for (const item of menuItems) {
    // 检查是否是主菜单项
    if (item.path === path) {
      crumbs.push({ path, label: item.label });
      found = true;
      break;
    }
    // 检查是否是子菜单项
    if (item.children) {
      const subItem = item.children.find(s => s.path === path);
      if (subItem) {
        crumbs.push({ path: item.path || '#', label: item.label });
        crumbs.push({ path, label: subItem.label });
        found = true;
        break;
      }
    }
  }
  // 如果没有找到对应的菜单项，只添加当前路径
  if (!found) {
    crumbs.push({ path, label: path.split('/').pop() || path });
  }
  return crumbs;
});

// 退出登录处理函数
const handleLogout = () => {
  // 实际项目中这里应该有退出登录的逻辑
  console.log('用户退出登录');
};

</script>



<style scoped>
.main-layout {
  display: flex;
  overflow: hidden;
  background: linear-gradient(135deg, #f5f7fa 0%, #e4e8f0 100%);
}

/* 侧边栏样式 */
.sidebar {
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
  background: white;
  border-right: 1px solid #f0f0f0;
  display: flex;
  flex-direction: column;
}

/* 带有子菜单的菜单项 */
.has-children>div>button {
  position: relative;
}

/* 子菜单箭头动画 */
.fa-chevron-down.rotate-180 {
  transform: rotate(180deg);
}

/* 子菜单样式 */
.has-children ul {
  border-left: 1px solid #f0f0f0;
  margin-left: 10px;
  animation: slideDown 0.2s ease-in-out;
}

@keyframes slideDown {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }

  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.sidebar-header {
  border-bottom: 1px solid #f0f0f0;
  background: #ffffff;
  flex-shrink: 0;
}

/* 菜单区域样式 - 添加滚动功能 */
.sidebar nav {
  max-height: calc(100vh - 120px);
  /* 减去头部高度，确保有足够空间 */
  overflow-y: auto;
  overflow-x: hidden;
  flex-grow: 1;
}

/* 滚动条样式优化 */
.sidebar nav::-webkit-scrollbar {
  width: 4px;
}

.sidebar nav::-webkit-scrollbar-track {
  background: #f1f1f1;
}

.sidebar nav::-webkit-scrollbar-thumb {
  background: #c1c1c1;
  border-radius: 2px;
}

.sidebar nav::-webkit-scrollbar-thumb:hover {
  background: #a8a8a8;
}

/* 菜单动画 */
.router-link {
  animation: slideInLeft 0.3s ease-out forwards;
  opacity: 0;
  transform: translateX(-20px);
}

@keyframes slideInLeft {
  to {
    opacity: 1;
    transform: translateX(0);
  }
}

/* 主内容区域 */
.main-content {
  flex: 1;
  overflow-y: auto;
  background: linear-gradient(135deg, #f5f7fa 0%, #e4e8f0 100%);
}

/* 滚动条样式 */
.main-content::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.main-content::-webkit-scrollbar-track {
  background: #f1f1f1;
  border-radius: 4px;
}

.main-content::-webkit-scrollbar-thumb {
  background: #c1c1c1;
  border-radius: 4px;
}

.main-content::-webkit-scrollbar-thumb:hover {
  background: #a8a8a8;
}

/* Font Awesome图标样式 */
.fas {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition: transform 0.3s ease;
}

/* 悬停效果 */
.router-link:hover .fas {
  transform: scale(1.1);
}

/* 动画效果 */
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(5px);
  }

  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.animate-fadeIn {
  animation: fadeIn 0.5s ease-out forwards;
}

/* 按钮悬停效果 */
button {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

button:hover {
  transform: translateY(-1px);
}

button:active {
  transform: translateY(0);
}

/* 响应式设计 */
@media (max-width: 768px) {
  .sidebar {
    transform: translateX(-100%);
    z-index: 50;
  }

  .sidebar.open {
    transform: translateX(0);
  }

  .main-content {
    margin-left: 0 !important;
  }
}
</style>