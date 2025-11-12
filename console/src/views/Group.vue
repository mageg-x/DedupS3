<template>
  <div class="groups-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">{{ t('group.pageTitle') }}</h1>
      <button @click="openAddGroupDialog"
        class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
        <i class="fas fa-plus"></i>
        <span>{{ t('group.addUserGroup') }}</span>
      </button>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" v-model="searchKeyword" :placeholder="t('group.searchPlaceholder')"
          class="w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 用户组列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('group.name') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('group.description') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('group.userCount') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('group.policyCount') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('group.creationTime') }}</th>
              <th class="px-6 py-3 text-right text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('group.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="group in filteredGroups" :key="group.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div
                    class="w-10 h-10 rounded-full bg-purple-100 flex items-center justify-center text-purple-600 font-medium">
                    {{ group.name.charAt(0).toUpperCase() }}
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ group.name }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 text-sm text-gray-500">
                {{ group.description || t('group.noDescription') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-sm font-medium rounded-full bg-blue-100 text-blue-800">
                  {{ group.users ? group.users.length : 0 }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-sm font-medium rounded-full bg-green-100 text-green-800">
                  {{ (group.policies || group.attachedPolicies) ? (group.policies || group.attachedPolicies).length : 0
                  }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(group.createAt || group.createdAt) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="openEditGroupDialog(group)"
                  class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-edit mr-1"></i>{{ t('group.edit') }}
                </button>
                <button @click="openDeleteGroupDialog(group.name)"
                  class="text-red-600 hover:text-red-900 transition-colors">
                  <i class="fas fa-trash-alt mr-1"></i>{{ t('group.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- 空状态 -->
      <div v-if="filteredGroups.length === 0" class="py-12 text-center">
        <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('group.noUserGroups') }}</h3>
        <p class="text-gray-500 mb-6">{{ t('group.clickAddUserGroup') }}</p>
      </div>
    </div>

    <!-- 添加/编辑用户组对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? t('group.editUserGroup') :
            t('group.addNewUserGroup') }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors"
            :aria-label="t('group.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <form @submit.prevent="submitForm">
            <div class="mb-4">
              <label for="groupName" class="block text-sm font-medium text-gray-700 mb-1">{{ t('group.name') }}</label>
              <input type="text" id="groupName" v-model="formData.name"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :placeholder="t('group.pleaseEnterGroupName')" required>
            </div>
            <div class="mb-4">
              <label for="description" class="block text-sm font-medium text-gray-700 mb-1">{{ t('group.description')
                }}</label>
              <textarea id="description" v-model="formData.description"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :placeholder="t('group.pleaseEnterGroupDescription')" rows="3"></textarea>
            </div>

            <!-- 关联用户 -->
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-2">{{ t('group.associatedUsers') }}</label>
              <div class="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto">
                <div v-for="user in usersList" :key="user.id" class="flex items-center">
                  <input type="checkbox" :id="`user-${user.id}`" :checked="formData.users.includes(user.id)"
                    @change="toggleUser(user.id)"
                    class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`user-${user.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ user.username }}
                    <span v-if="user.role === 'admin'" class="text-sm text-blue-500 ml-1">({{ t('group.admin')
                      }})</span>
                  </label>
                </div>
              </div>
            </div>

            <!-- 关联策略 -->
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-2">{{ t('group.associatedPolicies') }}</label>
              <div class="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto">
                <div v-for="policy in policiesList" :key="policy.id" class="flex items-center">
                  <input type="checkbox" :id="`policy-${policy.id}`" :checked="formData.policies.includes(policy.id)"
                    @change="togglePolicy(policy.id)"
                    class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`policy-${policy.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ policy.name }}
                  </label>
                </div>
              </div>
              <div v-if="policiesList.length === 0" class="text-gray-400 text-sm mt-1">{{ t('group.noAvailablePolicies')
                }}</div>
            </div>
          </form>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('common.cancel') }}
          </button>
          <button @click="submitForm"
            class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ isEditMode ? t('group.update') : t('group.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('group.confirmDelete') }}</h3>
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors"
            :aria-label="t('group.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('group.confirmDeleteMessage', { name: currentGroupId }) }}</p>
          <p v-if="usersInCurrentGroup > 0" class="text-sm text-red-500 mt-2">
            {{ t('group.userGroupUsersWarning', { count: usersInCurrentGroup }) }}
          </p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('group.cancel') }}
          </button>
          <button @click="confirmDeleteGroup"
            class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('group.confirmDelete') }}
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
import { listgroup, getgroup, creategroup, setgroup, delgroup, listuser, listpolicy } from '../api/admin.js';

// 获取翻译函数
const { t } = useI18n();

// ==================== 数据定义 ====================
const groupsList = ref([]);
const usersList = ref([]);
const policiesList = ref([]);

// 搜索关键字
const searchKeyword = ref('');

// 对话框状态
const dialogVisible = ref(false);
const isEditMode = ref(false);
const deleteDialogVisible = ref(false);
const currentGroupId = ref(null);
const usersInCurrentGroup = ref(0);

// 表单数据
const formData = ref({
  name: '',
  description: '',
  users: [],
  policies: []
});

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// ==================== 计算属性 ====================
const filteredGroups = computed(() => {
  if (!searchKeyword.value) {
    return groupsList.value;
  }
  return groupsList.value.filter(group =>
    group.name.toLowerCase().includes(searchKeyword.value.toLowerCase()) ||
    (group.description && group.description.toLowerCase().includes(searchKeyword.value.toLowerCase()))
  );
});

// ==================== 格式化函数 ====================
const formatDate = (date) => {
  try {
    // 检查日期是否有效
    if (!date || date === '0001-01-01T00:00:00Z' || date === '0001-01-01') {
      return t('group.noCreationTime');
    }

    let dateObj;
    if (date instanceof Date) {
      dateObj = date;
    } else {
      // 处理字符串类型的日期
      dateObj = new Date(date);
    }

    // 检查日期对象是否有效
    if (isNaN(dateObj.getTime())) {
      return t('group.invalidDate');
    }

    // 格式化日期为YYYY-MM-DD HH:mm:ss格式
    const year = dateObj.getFullYear();
    const month = String(dateObj.getMonth() + 1).padStart(2, '0');
    const day = String(dateObj.getDate()).padStart(2, '0');
    const hours = String(dateObj.getHours()).padStart(2, '0');
    const minutes = String(dateObj.getMinutes()).padStart(2, '0');
    const seconds = String(dateObj.getSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  } catch (error) {
    console.error('日期格式化失败:', error);
    return t('group.formatDateError');
  }
};

// ==================== API 数据加载 ====================
const loadGroupsFromAPI = async () => {
  try {
    const response = await listgroup();
    if (response.code === 0 && response.data) {
      // 转换数据结构以适应前端显示
      groupsList.value = response.data.map(group => ({
        ...group,
        // 为了兼容前端现有逻辑，将attachedPolicies转换为policies数组
        policies: group.attachedPolicies || [],
        // 为每个组添加id字段（使用name作为唯一标识符）
        id: group.name,
        // 兼容日期字段，将createAt转换为createdAt以适配模板
        createdAt: group.createAt
      }));
    } else {
      showNotification(response.msg || t('group.loadGroupsFailed'), 'error');
    }
  } catch (error) {
    console.error('加载用户组列表失败:', error);
    showNotification(t('group.loadGroupsFailed'), 'error');
  }
};

const loadUsersFromAPI = async () => {
  try {
    const response = await listuser();
    if (response.code === 0 && response.data) {
      usersList.value = response.data.map(user => ({
        ...user,
        id: user.username // 使用username作为id
      }));
    } else {
      showNotification(response.msg || t('group.loadUsersFailed'), 'error');
    }
  } catch (error) {
    console.error('加载用户列表失败:', error);
    showNotification(t('group.loadUsersFailed'), 'error');
  }
};

const loadPoliciesFromAPI = async () => {
  try {
    const response = await listpolicy();
    if (response.code === 0 && response.data) {
      policiesList.value = response.data.map(policy => ({
        ...policy,
        id: policy.name // 使用name作为id
      }));
    } else {
      showNotification(response.msg || t('group.loadPoliciesFailed'), 'error');
    }
  } catch (error) {
    console.error('加载策略列表失败:', error);
    showNotification(t('group.loadPoliciesFailed'), 'error');
  }
};

const loadAllData = async () => {
  try {
    await Promise.all([
      loadGroupsFromAPI(),
      loadUsersFromAPI(),
      loadPoliciesFromAPI()
    ]);
  } catch (error) {
    console.error('加载数据失败:', error);
  }
};

// ==================== 对话框操作 ====================
const openAddGroupDialog = () => {
  isEditMode.value = false;
  resetFormData();
  dialogVisible.value = true;
};

const openEditGroupDialog = (group) => {
  isEditMode.value = true;
  currentGroupId.value = group.name; // 使用name作为唯一标识符
  formData.value = {
    name: group.name,
    description: group.description || '',
    users: group.users || [],
    policies: group.attachedPolicies || []
  };
  dialogVisible.value = true;
};

const closeDialog = () => {
  dialogVisible.value = false;
  currentGroupId.value = null;
};

const openDeleteGroupDialog = (groupName) => {
  currentGroupId.value = groupName;
  const group = groupsList.value.find(g => g.name === groupName);
  if (group && group.users) {
    usersInCurrentGroup.value = group.users.length;
  }
  deleteDialogVisible.value = true;
};

const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentGroupId.value = null;
  usersInCurrentGroup.value = 0;
};

// ==================== 表单操作 ====================
const toggleUser = (userId) => {
  const index = formData.value.users.indexOf(userId);
  if (index > -1) {
    formData.value.users.splice(index, 1);
  } else {
    formData.value.users.push(userId);
  }
};

const togglePolicy = (policyId) => {
  const index = formData.value.policies.indexOf(policyId);
  if (index > -1) {
    formData.value.policies.splice(index, 1);
  } else {
    formData.value.policies.push(policyId);
  }
};

const resetFormData = () => {
  formData.value = {
    name: '',
    description: '',
    users: [],
    policies: []
  };
};

const submitForm = async () => {
  if (!formData.value.name) {
    showNotification(t('group.groupNameRequired'), 'error');
    return;
  }

  // 准备请求数据
  const requestData = {
    name: formData.value.name,
    desc: formData.value.description,
    users: formData.value.users,
    attachPolicies: formData.value.policies
  };

  try {
    let response;
    if (isEditMode.value) {
      // 编辑用户组
      response = await setgroup(requestData);
      if (response.code === 0) {
        showNotification(t('group.groupUpdated'), 'success');
        await loadGroupsFromAPI(); // 重新加载数据
      } else {
        showNotification(response.msg || t('group.updateGroupFailed'), 'error');
      }
    } else {
      // 添加用户组
      response = await creategroup(requestData);
      if (response.code === 0) {
        showNotification(t('group.groupCreated'), 'success');
        await loadGroupsFromAPI(); // 重新加载数据
      } else {
        showNotification(response.msg || t('group.createGroupFailed'), 'error');
      }
    }

    closeDialog();
  } catch (error) {
    console.error('提交表单失败:', error);
    showNotification(t('group.operationFailed'), 'error');
  }
};

// ==================== 删除操作 ====================
const confirmDeleteGroup = async () => {
  try {
    const response = await delgroup({ name: currentGroupId.value });
    if (response.code === 0) {
      showNotification(t('group.groupDeleted'), 'success');
      await loadGroupsFromAPI(); // 重新加载数据
    } else {
      showNotification(response.msg || t('group.deleteGroupFailed'), 'error');
    }
  } catch (error) {
    console.error('删除用户组失败:', error);
    showNotification(t('group.operationFailed'), 'error');
  }
  closeDeleteDialog();
};

// ==================== 通知提示 ====================
const showNotification = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  // 3秒后自动隐藏
  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

// ==================== 组件挂载 ====================
onMounted(() => {
  loadAllData();
});
</script>

<style scoped>
.groups-container {
  max-width: 1200px;
  margin: 0 auto;
}

.page-header {
  margin-bottom: 2rem;
}

.search-container {
  margin-bottom: 1.5rem;
}

.card {
  background: white;
  border-radius: 0.75rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  overflow: hidden;
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
  .groups-container {
    padding: 1rem;
  }

  .page-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 1rem;
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

  .dialog-container {
    margin: 1rem;
    width: calc(100% - 2rem);
  }

  .grid-cols-2 {
    grid-template-columns: 1fr;
  }
}
</style>