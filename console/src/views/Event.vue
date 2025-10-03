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
          class=" text-sm w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 时间范围选择 -->
    <div class="date-filter mb-6 flex flex-wrap gap-4">
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('event.startTime') }}:</label>
        <el-date-picker
          v-model="startDate"
          type="datetime"
          :placeholder="t('event.selectStartTime')"
          class="text-sm"
          style="width: 200px"
        />
      </div>
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('event.endTime') }}:</label>
        <el-date-picker
          v-model="endDate"
          type="datetime"
          :placeholder="t('event.selectEndTime')"
          class="text-sm"
          style="width: 200px"
        />
      </div>
      <div class="flex-1 flex gap-2">
        <button @click="setToday"
          class="px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm">{{ t('event.today') }}</button>
        <button @click="setYesterday"
          class="px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm">{{ t('event.yesterday') }}</button>
        <button @click="setLast7Days"
          class="px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm">{{ t('event.last7Days') }}</button>
      </div>
    </div>

    <!-- 事件列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.time') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.eventType') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.username') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.ipAddress') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.description') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.status') }}</th>
              <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('event.operation') }}</th>
            </tr>
          </thead>
          <tbody class="text-sm bg-white divide-y divide-gray-200">
            <tr v-for="event in paginatedEvents" :key="event.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(event.timestamp) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="['px-2 py-1 text-xs rounded-full', getEventTypeClass(event.type)]">
                  {{ getEventTypeName(event.type) }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                {{ event.username || '系统' }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ event.ipAddress || 'N/A' }}
              </td>
              <td class="px-6 py-4 text-sm text-gray-700">
                {{ event.description }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span
                  :class="['px-2 py-1 text-xs rounded-full', event.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800']">
                  {{ event.status === 'success' ? t('event.success') : t('event.failed') }}
                </span>
              </td>
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
        <div class="text-gray-400 mb-4">
          <i class="fas fa-bell-slash text-4xl"></i>
        </div>
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
          <button v-if="page === '...'" class="px-2 py-1 text-sm text-gray-500" disabled>
            ...
          </button>
          <button v-else @click="changePage(page)" class="px-2 py-1 text-sm rounded-md transition-colors"
            :class="currentPage === page ? 'bg-blue-100 text-blue-800' : 'hover:bg-gray-100'">
            {{ page }}
          </button>
        </span>
        <button @click="changePage(currentPage + 1)" :disabled="currentPage === totalPages"
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
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('event.status') }}:</span>
              <span
                :class="['px-2 py-1 text-xs rounded-full', currentEvent.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800']">
                {{ currentEvent.status === 'success' ? t('event.success') : t('event.failed') }}
              </span>
            </div>
            <div class="">
              <div class="text-gray-500 mb-1">{{ t('event.description') }}:</div>
              <div class="p-3 bg-gray-50 rounded-lg text-sm">{{ currentEvent.description }}</div>
            </div>
            <div v-if="currentEvent.details" class="">
              <div class="text-gray-500 mb-1">{{ t('event.detailInfo') }}:</div>
              <div class="p-3 bg-gray-50 rounded-lg text-sm font-mono whitespace-pre-wrap">
                {{ JSON.stringify(currentEvent.details, null, 2) }}
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
    <div v-if="showToast"
      :class="['fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg transition-all duration-300 z-50', toastType === 'success' ? 'bg-green-500 text-white' : 'bg-red-500 text-white']">
      <div class="flex items-center gap-2">
        <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
        <i v-else class="fas fa-exclamation-circle"></i>
        <span>{{ toastMessage }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElDatePicker } from 'element-plus';

const { t } = useI18n();

// 模拟事件数据
const eventsList = ref([]);

// 搜索和筛选
const searchKeyword = ref('');
const eventTypeFilter = ref('all');
const startDate = ref('');
const endDate = ref('');

// 分页相关
const currentPage = ref(1);
const pageSize = ref(10);

// 详情对话框
const detailsVisible = ref(false);
const currentEvent = ref({});

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 生成模拟事件数据
const generateMockEvents = () => {
  const now = new Date();
  const eventTypes = ['login', 'bucket', 'object', 'user', 'policy'];
  const users = ['admin', 'developer', 'tester', 'system'];
  const ipAddresses = ['192.168.1.100', '10.0.0.5', '172.16.0.23', '127.0.0.1'];
  const statuses = ['success', 'failure'];

  // 需要在语言文件中添加eventType对象和这些模板的翻译键
  const eventTemplates = {
    login: [
      t('eventTemplate.loginSuccess', { username: '{username}' }),
      t('eventTemplate.loginFailedPassword', { username: '{username}' }),
      t('eventTemplate.loginFailedLocked', { username: '{username}' })
    ],
    bucket: [
      t('eventTemplate.bucketCreated', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.bucketDeleted', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.bucketListed', { username: '{username}' }),
      t('eventTemplate.bucketAclModified', { username: '{username}', resource: '{resource}' })
    ],
    object: [
      t('eventTemplate.objectUploaded', { username: '{username}', resource: '{resource}', bucket: '{bucket}' }),
      t('eventTemplate.objectDownloaded', { username: '{username}', resource: '{resource}', bucket: '{bucket}' }),
      t('eventTemplate.objectDeleted', { username: '{username}', resource: '{resource}', bucket: '{bucket}' }),
      t('eventTemplate.objectsListed', { username: '{username}', bucket: '{bucket}' })
    ],
    user: [
      t('eventTemplate.userCreated', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.userPermissionModified', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.userDeleted', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.userPasswordReset', { username: '{username}', resource: '{resource}' })
    ],
    policy: [
      t('eventTemplate.policyCreated', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.policyModified', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.policyDeleted', { username: '{username}', resource: '{resource}' }),
      t('eventTemplate.policyAssigned', { username: '{username}', user: '{user}', resource: '{resource}' })
    ]
  };

  const buckets = ['documents', 'images', 'logs', 'backup', 'media'];
  const resources = ['file1.txt', 'image.jpg', 'data.json', 'config.xml', 'report.pdf'];
  const newUsers = ['bob', 'alice', 'charlie', 'david'];
  const newPolicies = ['read-only', 'admin-policy', 'developer-policy', 'test-policy'];

  const events = [];

  // 生成过去30天的事件
  for (let i = 0; i < 100; i++) {
    const daysAgo = Math.floor(Math.random() * 30);
    const hoursAgo = Math.floor(Math.random() * 24);
    const minutesAgo = Math.floor(Math.random() * 60);
    const timestamp = new Date(now);
    timestamp.setDate(timestamp.getDate() - daysAgo);
    timestamp.setHours(timestamp.getHours() - hoursAgo);
    timestamp.setMinutes(timestamp.getMinutes() - minutesAgo);

    const type = eventTypes[Math.floor(Math.random() * eventTypes.length)];
    const username = users[Math.floor(Math.random() * users.length)];
    const ipAddress = ipAddresses[Math.floor(Math.random() * ipAddresses.length)];
    const status = statuses[Math.floor(Math.random() * statuses.length)];

    // 根据事件类型生成描述
    const templates = eventTemplates[type];
    const template = templates[Math.floor(Math.random() * templates.length)];

    let description = template;
    let details = {};

    // 替换模板中的变量
    if (template.includes('{username}')) {
      description = description.replace('{username}', username);
    }
    if (template.includes('{resource}')) {
      let resource = '';
      if (type === 'bucket') resource = buckets[Math.floor(Math.random() * buckets.length)];
      else if (type === 'object') resource = resources[Math.floor(Math.random() * resources.length)];
      else if (type === 'user') resource = newUsers[Math.floor(Math.random() * newUsers.length)];
      else if (type === 'policy') resource = newPolicies[Math.floor(Math.random() * newPolicies.length)];
      description = description.replace('{resource}', resource);
      details.resource = resource;
    }
    if (template.includes('{bucket}')) {
      const bucket = buckets[Math.floor(Math.random() * buckets.length)];
      description = description.replace('{bucket}', bucket);
      details.bucket = bucket;
    }
    if (template.includes('{user}')) {
      const user = newUsers[Math.floor(Math.random() * newUsers.length)];
      description = description.replace('{user}', user);
      details.user = user;
    }

    // 添加一些随机的详情
    if (Math.random() > 0.5) {
      details.sessionId = 'session-' + Math.random().toString(36).substr(2, 9);
    }
    if (Math.random() > 0.5) {
      details.userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36';
    }

    events.push({
      id: i + 1,
      timestamp: timestamp,
      type: type,
      username: username !== 'system' ? username : null,
      ipAddress: ipAddress,
      description: description,
      status: status,
      details: details
    });
  }

  // 按时间倒序排列
  events.sort((a, b) => b.timestamp - a.timestamp);
  return events;
};

// 加载模拟数据
const loadMockData = () => {
  eventsList.value = generateMockEvents();
};

// 格式化日期
const formatDate = (date) => {
  if (!(date instanceof Date)) {
    date = new Date(date);
  }
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
};

// 获取事件类型名称
const getEventTypeName = (type) => {
  const typeNames = {
    'login': t('eventType.login'),
    'bucket': t('eventType.bucket'),
    'object': t('eventType.object'),
    'user': t('eventType.user'),
    'policy': t('eventType.policy')
  };
  return typeNames[type] || type;
};

// 获取事件类型样式
const getEventTypeClass = (type) => {
  const typeClasses = {
    'login': 'bg-blue-100 text-blue-800',
    'bucket': 'bg-green-100 text-green-800',
    'object': 'bg-purple-100 text-purple-800',
    'user': 'bg-orange-100 text-orange-800',
    'policy': 'bg-pink-100 text-pink-800'
  };
  return typeClasses[type] || 'bg-gray-100 text-gray-800';
};

// 过滤事件列表
const filteredEvents = computed(() => {
  return eventsList.value.filter(event => {
    // 类型过滤
    if (eventTypeFilter.value !== 'all' && event.type !== eventTypeFilter.value) {
      return false;
    }

    // 关键词搜索
    if (searchKeyword.value) {
      const keyword = searchKeyword.value.toLowerCase();
      const matchesKeyword =
        (event.username && event.username.toLowerCase().includes(keyword)) ||
        event.description.toLowerCase().includes(keyword) ||
        (event.ipAddress && event.ipAddress.includes(keyword)) ||
        (event.details && event.details.resource && event.details.resource.toLowerCase().includes(keyword)) ||
        (event.details && event.details.bucket && event.details.bucket.toLowerCase().includes(keyword));
      if (!matchesKeyword) {
        return false;
      }
    }

    // 时间范围过滤
    if (startDate.value) {
      const start = new Date(startDate.value);
      if (event.timestamp < start) {
        return false;
      }
    }

    if (endDate.value) {
      const end = new Date(endDate.value);
      // 结束时间设置为当天的23:59:59
      end.setHours(23, 59, 59, 999);
      if (event.timestamp > end) {
        return false;
      }
    }

    return true;
  });
});

// 计算分页后的事件记录
const paginatedEvents = computed(() => {
  const startIndex = (currentPage.value - 1) * pageSize.value;
  const endIndex = startIndex + pageSize.value;
  return filteredEvents.value.slice(startIndex, endIndex);
});

// 计算总页数
const totalPages = computed(() => {
  return Math.ceil(filteredEvents.value.length / pageSize.value);
});

// 切换到指定页码
const changePage = (page) => {
  if (page >= 1 && page <= totalPages.value) {
    currentPage.value = page;
  }
};

// 切换每页显示数量
const changePageSize = (size) => {
  pageSize.value = size;
  currentPage.value = 1; // 重置到第一页
};

// 预设时间范围
const setToday = () => {
  const today = new Date();
  const startOfDay = new Date(today);
  startOfDay.setHours(0, 0, 0, 0);
  const endOfDay = new Date(today);
  endOfDay.setHours(23, 59, 59, 999);

  startDate.value = startOfDay;
  endDate.value = endOfDay;
};

const setYesterday = () => {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const startOfDay = new Date(yesterday);
  startOfDay.setHours(0, 0, 0, 0);
  const endOfDay = new Date(yesterday);
  endOfDay.setHours(23, 59, 59, 999);

  startDate.value = startOfDay;
  endDate.value = endOfDay;
};

const setLast7Days = () => {
  const today = new Date();
  const sevenDaysAgo = new Date(today);
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
  sevenDaysAgo.setHours(0, 0, 0, 0);
  const endOfDay = new Date(today);
  endOfDay.setHours(23, 59, 59, 999);

  startDate.value = sevenDaysAgo;
  endDate.value = endOfDay;
};

const setLast30Days = () => {
  const today = new Date();
  const thirtyDaysAgo = new Date(today);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  thirtyDaysAgo.setHours(0, 0, 0, 0);
  const endOfDay = new Date(today);
  endOfDay.setHours(23, 59, 59, 999);

  startDate.value = thirtyDaysAgo;
  endDate.value = endOfDay;
};

// 清除筛选
const clearFilter = () => {
  searchKeyword.value = '';
  eventTypeFilter.value = 'all';
  startDate.value = '';
  endDate.value = '';
};

// 显示事件详情
const showEventDetails = (event) => {
  currentEvent.value = event;
  detailsVisible.value = true;
};

// 关闭详情对话框
const closeDetails = () => {
  detailsVisible.value = false;
  currentEvent.value = {};
};

// 显示提示消息
const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  // 3秒后自动隐藏
  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

// 计算显示的页码
const displayPages = computed(() => {
  const pages = [];
  const total = totalPages.value;
  const current = currentPage.value;

  // 总是显示第一页
  pages.push(1);

  // 如果当前页离第一页超过2页，显示省略号
  if (current > 4) {
    pages.push('...');
  }

  // 显示当前页附近的页码
  const start = Math.max(2, current - 1);
  const end = Math.min(total - 1, current + 1);

  for (let i = start; i <= end; i++) {
    if (!pages.includes(i)) {
      pages.push(i);
    }
  }

  // 如果最后一页离当前页超过2页，显示省略号
  if (current < total - 3) {
    pages.push('...');
  }

  // 总是显示最后一页（如果不是第一页）
  if (total > 1) {
    if (!pages.includes(total)) {
      pages.push(total);
    }
  }

  return pages;
});

// 组件挂载时加载数据
onMounted(() => {
  loadMockData();
});
</script>

<style scoped>
.events-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 2rem;
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