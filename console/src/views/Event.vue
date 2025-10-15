<template>
  <div class="events-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">{{ t('event.pageTitle') }}</h1>
      <div class="flex items-center gap-3">
        <div class="relative">
          <select v-model="eventTypeFilter"
            class="appearance-none text-sm bg-white border border-gray-300 rounded-lg pl-4 pr-10 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
            <option value="all">{{ t('event.allTypes') }}</option>
            <option value="login">{{ t('event.loginEvents') }}</option>
            <option value="bucket">{{ t('event.bucketOperations') }}</option>
            <option value="object">{{ t('event.objectOperations') }}</option>
            <option value="user">{{ t('event.userManagement') }}</option>
            <option value="policy">{{ t('event.policyManagement') }}</option>
            <option value="group">{{ t('eventType.group') }}</option>
            <option value="role">{{ t('eventType.role') }}</option>
            <option value="accessKey">{{ t('eventType.accessKey') }}</option>
            <option value="quota">{{ t('eventType.quota') }}</option>
            <option value="chunk">{{ t('eventType.chunk') }}</option>
            <option value="storage">{{ t('eventType.storage') }}</option>
            <option value="debug">{{ t('eventType.debug') }}</option>
            <option value="system">{{ t('eventType.system') }}</option>
          </select>
          <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-500">
            <i class="fas fa-chevron-down text-xs"></i>
          </div>
        </div>
      </div>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" v-model="searchKeyword" :placeholder="t('event.searchPlaceholder')"
          class="text-sm w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" />
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 时间范围选择 -->
    <div class="date-filter mb-6 flex flex-wrap gap-4">
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('event.startTime') }}:</label>
        <el-date-picker v-model="startDate" type="datetime" :placeholder="t('common.selectStartTime')" class="text-sm"
          style="width: 200px" />
      </div>
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('event.endTime') }}:</label>
        <el-date-picker v-model="endDate" type="datetime" :placeholder="t('common.selectEndTime')" class="text-sm"
          style="width: 200px" />
      </div>
      <div class="flex-1 flex gap-2">
        <button @click="setToday"
          class="px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm">
          {{ t('event.today') }}
        </button>
        <button @click="setYesterday"
          class="px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm">
          {{ t('event.yesterday') }}
        </button>
        <button @click="setLast7Days"
          class="px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm">
          {{ t('event.last7Days') }}
        </button>
      </div>
    </div>

    <!-- 事件列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('event.time') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('event.eventType') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('event.username') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('event.ipAddress') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('event.description') }}</th>
              <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('event.operation') }}</th>
            </tr>
          </thead>
          <tbody class="text-sm bg-white divide-y divide-gray-200">
            <tr v-if="loading" class="text-center">
              <td colspan="7" class="px-6 py-8">
                <i class="fas fa-spinner fa-spin text-blue-500 mr-2"></i>
                {{ t('common.loading') }}
              </td>
            </tr>
            <tr v-else v-for="event in paginatedEvents" :key="event.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{{ formatDate(event.timestamp) }}</td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="['px-2 py-1 text-xs rounded-full', getEventTypeClass(event.type)]">
                  {{ getEventTypeName(event.type) }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{{ event.username || '系统' }}</td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{{ event.ipAddress || 'N/A' }}</td>
              <td class="px-6 py-4 text-sm text-gray-700">{{ event.description }}</td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showEventDetails(event)" class="text-blue-600 hover:text-blue-900 transition-colors">
                  <i class="fas fa-eye mr-1"></i>{{ t('event.detail') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 空状态 -->
      <div v-if="filteredEvents.length === 0" class="py-12 text-center">
        <div class="text-gray-400 mb-4"><i class="fas fa-bell-slash text-4xl"></i></div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('event.noEvents') }}</h3>
        <p class="text-gray-500 mb-6">{{ t('event.systemWillRecord') }}</p>
      </div>
    </div>

    <!-- 分页控件 -->
    <div v-if="totalPages > 0" class="flex items-center justify-between mt-4">
      <div class="flex items-center space-x-2">
        <span class="text-sm text-gray-600">{{ t('event.totalRecords', { total: filteredEvents.length }) }}</span>
        <select v-model="pageSize" @change="changePageSize" class="border border-gray-300 rounded-md text-sm">
          <option value="10">{{ t('event.page10') }}</option>
          <option value="20">{{ t('event.page20') }}</option>
          <option value="50">{{ t('event.page50') }}</option>
        </select>
      </div>
      <div class="flex items-center space-x-1">
        <button @click="changePage(currentPage - 1)" :disabled="currentPage === 1"
          class="px-3 py-1 text-sm border rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          <i class="fas fa-chevron-left"></i>
        </button>
        <span v-for="page in displayPages" :key="page" class="px-3 py-1 text-sm">
          <button v-if="page === '...'" class="px-2 py-1 text-sm text-gray-500" disabled>...</button>
          <button v-else @click="changePage(page)" class="px-2 py-1 text-sm rounded-md transition-colors"
            :class="currentPage === page ? 'bg-blue-100 text-blue-800' : 'hover:bg-gray-100'">
            {{ page }}
          </button>
        </span>
        <button @click="changePage(currentPage + 1)" :disabled="!hasMore && currentPage === totalPages"
          class="px-3 py-1 text-sm border rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          <i class="fas fa-chevron-right"></i>
        </button>
      </div>
    </div>

    <!-- 事件详情对话框 -->
    <div v-if="detailsVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-2xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('event.eventDetails') }}</h3>
          <button @click="closeDetails" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <div class="space-y-4">
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('event.time') }}:</span>
              <span class="font-medium">{{ formatDate(currentEvent.timestamp) }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('event.eventType') }}:</span>
              <span :class="['px-2 py-1 text-xs rounded-full', getEventTypeClass(currentEvent.type)]">
                {{ getEventTypeName(currentEvent.type) }}
              </span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('event.username') }}:</span>
              <span class="font-medium">{{ currentEvent.username || '系统' }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('event.ipAddress') }}:</span>
              <span class="font-medium">{{ currentEvent.ipAddress || 'N/A' }}</span>
            </div>
            <div>
              <div class="text-gray-500 mb-1">{{ t('event.description') }}:</div>
              <div class="p-3 bg-gray-50 rounded-lg text-sm">{{ currentEvent.description }}</div>
            </div>
            <div v-if="currentEvent.context">
              <div class="text-gray-500 mb-1">{{ t('event.detailInfo') }}:</div>
              <div class="p-3 bg-gray-50 rounded-lg text-sm font-mono whitespace-pre-wrap">
                {{ JSON.stringify(currentEvent.context, null, 2) }}
              </div>
            </div>
          </div>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end">
          <button @click="closeDetails"
            class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ t('event.close') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 操作结果提示 -->
    <div v-if="showToast" :class="[
      'fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg transition-all duration-300 z-50',
      toastType === 'success' ? 'bg-green-500 text-white' : 'bg-red-500 text-white'
    ]">
      <div class="flex items-center gap-2">
        <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
        <i v-else class="fas fa-exclamation-circle"></i>
        <span>{{ toastMessage }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElDatePicker } from 'element-plus';
import { listeventlog } from '@/api/admin.js';

const { t } = useI18n();

// ======================
// 响应式状态
// ======================
const hasMore = ref(false);
const eventsList = ref([]);
const totalCount = ref(0);

const searchKeyword = ref('');
const eventTypeFilter = ref('all');
const startDate = ref('');
const endDate = ref('');

const currentPage = ref(1);
const pageSize = ref(10);

const detailsVisible = ref(false);
const currentEvent = ref({});

const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

const loading = ref(false);

// ======================
// 工具函数
// ======================
const formatDate = (date) => {
  if (!(date instanceof Date)) date = new Date(date);
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
};

const getEventTypeName = (type) => {
  const typeNames = {
    login: t('eventType.login'),
    bucket: t('eventType.bucket'),
    object: t('eventType.object'),
    user: t('eventType.user'),
    policy: t('eventType.policy'),
    group: t('eventType.group'),
    role: t('eventType.role'),
    accessKey: t('eventType.accessKey'),
    quota: t('eventType.quota'),
    chunk: t('eventType.chunk'),
    storage: t('eventType.storage'),
    debug: t('eventType.debug'),
    system: t('eventType.system')
  };
  return typeNames[type] || type;
};

const getEventTypeClass = (type) => {
  const typeClasses = {
    login: 'bg-blue-100 text-blue-800',
    bucket: 'bg-green-100 text-green-800',
    object: 'bg-purple-100 text-purple-800',
    user: 'bg-orange-100 text-orange-800',
    policy: 'bg-pink-100 text-pink-800',
    group: 'bg-teal-100 text-teal-800',
    role: 'bg-indigo-100 text-indigo-800',
    accessKey: 'bg-yellow-100 text-yellow-800',
    quota: 'bg-amber-100 text-amber-800',
    chunk: 'bg-cyan-100 text-cyan-800',
    storage: 'bg-lime-100 text-lime-800',
    debug: 'bg-rose-100 text-rose-800',
    system: 'bg-violet-100 text-violet-800'
  };
  return typeClasses[type] || 'bg-gray-100 text-gray-800';
};

// ======================
// 数据映射
// ======================
const mapApiDataToFrontend = (apiData) => {
  const eventName = apiData.eventName || '';
  let type = 'system';

  if (eventName.startsWith('login') || eventName.startsWith('console:Login') || eventName.startsWith('console:Logout')) {
    type = 'login';
  } else if (
    eventName.startsWith('console:GetUserInfo') ||
    eventName.startsWith('console:ListUsers') ||
    eventName.startsWith('console:CreateUser') ||
    eventName.startsWith('console:UpdateUser') ||
    eventName.startsWith('console:DeleteUser')
  ) {
    type = 'user';
  } else if (
    eventName.startsWith('console:ListBuckets') ||
    eventName.startsWith('console:CreateBucket') ||
    eventName.startsWith('console:DeleteBucket') ||
    eventName.startsWith('s3:CreateBucket') ||
    eventName.startsWith('s3:DeleteBucket')
  ) {
    type = 'bucket';
  } else if (
    eventName.startsWith('console:ListObjects') ||
    eventName.startsWith('console:CreateFolder') ||
    eventName.startsWith('console:PutObject') ||
    eventName.startsWith('console:DeleteObject') ||
    eventName.startsWith('console:GetObject') ||
    eventName.startsWith('s3:')
  ) {
    type = 'object';
  } else if (
    eventName.startsWith('console:ListPolicies') ||
    eventName.startsWith('console:GetPolicy') ||
    eventName.startsWith('console:CreatePolicy') ||
    eventName.startsWith('console:UpdatePolicy') ||
    eventName.startsWith('console:DeletePolicy')
  ) {
    type = 'policy';
  } else if (
    eventName.startsWith('console:ListGroups') ||
    eventName.startsWith('console:GetGroup') ||
    eventName.startsWith('console:CreateGroup') ||
    eventName.startsWith('console:UpdateGroup') ||
    eventName.startsWith('console:DeleteGroup')
  ) {
    type = 'group';
  } else if (
    eventName.startsWith('console:ListRoles') ||
    eventName.startsWith('console:GetRole') ||
    eventName.startsWith('console:CreateRole') ||
    eventName.startsWith('console:UpdateRole') ||
    eventName.startsWith('console:DeleteRole')
  ) {
    type = 'role';
  } else if (
    eventName.startsWith('console:ListAccessKeys') ||
    eventName.startsWith('console:CreateAccessKey') ||
    eventName.startsWith('console:UpdateAccessKey') ||
    eventName.startsWith('console:DeleteAccessKey')
  ) {
    type = 'accessKey';
  } else if (
    eventName.startsWith('console:ListQuotas') ||
    eventName.startsWith('console:CreateQuota') ||
    eventName.startsWith('console:UpdateQuota') ||
    eventName.startsWith('console:DeleteQuota')
  ) {
    type = 'quota';
  } else if (
    eventName.startsWith('console:ListChunkConfigs') ||
    eventName.startsWith('console:GetChunkConfig') ||
    eventName.startsWith('console:UpdateChunkConfig')
  ) {
    type = 'chunk';
  } else if (
    eventName.startsWith('console:ListStorages') ||
    eventName.startsWith('console:CreateStorage') ||
    eventName.startsWith('console:TestStorage') ||
    eventName.startsWith('console:DeleteStorage')
  ) {
    type = 'storage';
  } else if (eventName.startsWith('console:Debug')) {
    type = 'debug';
  }

  const username = apiData.userIdentity?.userName || apiData.userIdentity?.principalId || 'system';
  let description = t('auditTypes.' + eventName) || eventName;

  if (apiData.s3?.bucket?.name) {
    if (apiData.s3?.object?.key) {
      description += `: ${apiData.s3.bucket.name}/${apiData.s3.object.key}`;
    } else {
      description += `: ${apiData.s3.bucket.name}`;
    }
  }

  const ipAddress = apiData.sourceIPAddress || 'N/A';

  return {
    id: apiData.eventID || apiData.id,
    timestamp: apiData.eventTime,
    type,
    username: username !== 'system' ? username : null,
    ipAddress,
    description,
    context: apiData
  };
};

// ======================
// 数据加载
// ======================
const loadEventData = async () => {
  try {
    loading.value = true;

    let startTime, endTime;
    if (startDate.value && endDate.value) {
      startTime = Math.floor(new Date(startDate.value).getTime() / 1000);
      endTime = Math.floor(new Date(endDate.value).getTime() / 1000);
    } else {
      const now = new Date();
      endTime = Math.floor(now.getTime() / 1000);
      startTime = Math.floor((now.getTime() - 7 * 24 * 60 * 60 * 1000) / 1000);
    }

    const params = {
      startTime: startTime.toString(),
      endTime: endTime.toString(),
      eventName: eventTypeFilter.value === 'all' ? '' : eventTypeFilter.value,
      offset: ((currentPage.value - 1) * pageSize.value).toString(),
      limit: pageSize.value.toString()
    };

    const response = await listeventlog(params);
    if (response.code === 0) {
      eventsList.value = response.data.records.map(mapApiDataToFrontend);
      totalCount.value = response.data.total;
      // 处理hasMore字段，表示是否有更多数据未拉取
      hasMore.value = response.data.hasMore || false;
    } else {
      showToastMessage(`加载事件数据失败: ${response.msg}`, 'error');
    }
  } catch (error) {
    console.error('加载事件数据错误:', error);
    showToastMessage('加载事件数据失败', 'error');
  } finally {
    loading.value = false;
  }
};

// ======================
// 计算属性
// ======================
const filteredEvents = computed(() => {
  return eventsList.value.filter((event) => {
    if (eventTypeFilter.value !== 'all' && event.type !== eventTypeFilter.value) return false;
    if (searchKeyword.value) {
      const keyword = searchKeyword.value.toLowerCase();
      const matches =
        (event.username && event.username.toLowerCase().includes(keyword)) ||
        event.description.toLowerCase().includes(keyword) ||
        (event.ipAddress && event.ipAddress.includes(keyword));
      if (!matches) return false;
    }
    if (startDate.value && new Date(event.timestamp) < new Date(startDate.value)) return false;
    if (endDate.value) {
      const end = new Date(endDate.value);
      end.setHours(23, 59, 59, 999);
      if (new Date(event.timestamp) > end) return false;
    }
    return true;
  });
});

const paginatedEvents = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value;
  return filteredEvents.value.slice(start, start + pageSize.value);
});

const totalPages = computed(() => Math.ceil(totalCount.value / pageSize.value) || 1);

const displayPages = computed(() => {
  const pages = [];
  const total = totalPages.value;
  const current = currentPage.value;

  pages.push(1);

  if (current > 4) pages.push('...');
  const start = Math.max(2, current - 1);
  const end = Math.min(total - 1, current + 1);
  for (let i = start; i <= end; i++) if (!pages.includes(i)) pages.push(i);
  if (current < total - 3) pages.push('...');
  if (total > 1 && !pages.includes(total)) pages.push(total);

  return pages;
});

// ======================
// UI 交互
// ======================
const changePage = (page) => {
  // 只有当页码有效且有更多数据或未到达最后一页时才允许切换页码
  if (page >= 1 && (hasMore.value || page <= totalPages.value)) {
    currentPage.value = page;
  }
};

const changePageSize = () => {
  currentPage.value = 1;
};

const setToday = () => {
  const today = new Date();
  startDate.value = new Date(today.setHours(0, 0, 0, 0));
  endDate.value = new Date(today.setHours(23, 59, 59, 999));
};

const setYesterday = () => {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  startDate.value = new Date(yesterday.setHours(0, 0, 0, 0));
  endDate.value = new Date(yesterday.setHours(23, 59, 59, 999));
};

const setLast7Days = () => {
  const today = new Date();
  const sevenDaysAgo = new Date(today);
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
  startDate.value = new Date(sevenDaysAgo.setHours(0, 0, 0, 0));
  endDate.value = new Date(today.setHours(23, 59, 59, 999));
};

const showEventDetails = (event) => {
  currentEvent.value = event;
  detailsVisible.value = true;
};

const closeDetails = () => {
  detailsVisible.value = false;
  currentEvent.value = {};
};

const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;
  setTimeout(() => (showToast.value = false), 3000);
};

// ======================
// 生命周期 & 监听
// ======================
onMounted(() => {
  setToday();
  loadEventData();
});

watch([searchKeyword, eventTypeFilter, startDate, endDate], () => {
  currentPage.value = 1;
  loadEventData();
});
</script>


<style scoped>
.events-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 2rem 1rem;
}

.page-header {
  margin-bottom: 1.5rem;
}

.search-container {
  margin-bottom: 1.5rem;
}

.date-filter {
  margin-bottom: 1.5rem;
}

.card {
  background: white;
  border-radius: 0.75rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  overflow: hidden;
}

/* 分页控件样式 */
.pagination {
  display: flex;
  align-items: center;
  justify-content: center;
  margin-top: 1rem;
}

.pagination button {
  display: flex;
  align-items: center;
  justify-content: center;
  min-width: 2rem;
  height: 2rem;
  padding: 0 0.5rem;
  border: 1px solid #e2e8f0;
  background-color: white;
  color: #64748b;
  font-size: 0.875rem;
  cursor: pointer;
  transition: all 0.2s ease;
}

.pagination button:hover:not(:disabled) {
  background-color: #f8fafc;
  border-color: #cbd5e1;
}

.pagination button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.pagination button.active {
  background-color: #3b82f6;
  border-color: #3b82f6;
  color: white;
}

.pagination select {
  padding: 0.25rem 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 0.375rem;
  background-color: white;
  font-size: 0.875rem;
}

/* 动画效果 */
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }

  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.animate-fadeIn {
  animation: fadeIn 0.3s ease-out;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .events-container {
    padding: 1rem;
  }

  .page-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 1rem;
  }

  .date-filter {
    flex-direction: column;
  }

  .date-filter .flex-1 {
    justify-content: flex-start;
  }

  .card {
    margin: -1rem;
    border-radius: 0;
  }

  table {
    font-size: 0.875rem;
  }

  th,
  td {
    padding: 0.75rem !important;
  }
}
</style>