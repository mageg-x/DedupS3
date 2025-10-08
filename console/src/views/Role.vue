<template>
  <div class="roles-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">{{ t('role.pageTitle') }}</h1>
      <button @click="showAddRoleDialog"
        class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
        <i class="fas fa-plus"></i>
        <span>{{ t('role.addRole') }}</span>
      </button>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" v-model="searchKeyword" :placeholder="t('role.searchPlaceholder')"
          class="w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 角色列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('role.name') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('role.description') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">{{
                t('role.associatedPolicies') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('role.creationTime') }}</th>
              <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">{{
                t('role.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="role in filteredRoles" :key="role.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div
                    class="w-10 h-10 rounded-full bg-orange-100 flex items-center justify-center text-orange-600 font-medium">
                    <i class="fas fa-user-tag"></i>
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ role.name }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 text-sm text-gray-500">
                {{ role.description || t('role.noDescription') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-xs font-medium rounded-full bg-green-100 text-green-800">
                  {{ role.policies ? role.policies.length : 0 }}
                </span>
              </td>

              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(role.createAt || role.createdAt) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showRoleDetails(role)"
                  class="text-green-600 hover:text-green-900 transition-colors mr-3">
                  <i class="fas fa-eye mr-1"></i>{{ t('role.view') }}
                </button>
                <button @click="showEditRoleDialog(role)"
                  class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-edit mr-1"></i>{{ t('role.edit') }}
                </button>
                <button @click="handleDeleteRole(role.name)" class="text-red-600 hover:text-red-900 transition-colors">
                  <i class="fas fa-trash-alt mr-1"></i>{{ t('role.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- 空状态 -->
      <div v-if="filteredRoles.length === 0" class="py-12 text-center">
        <div class="text-gray-400 mb-4">
          <i class="fas fa-user-tag-slash text-4xl"></i>
        </div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('role.noRoles') }}</h3>
        <p class="text-gray-500 mb-6">{{ t('role.clickAddRole') }}</p>
      </div>
    </div>

    <!-- 添加/编辑角色对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? t('role.editRole') : t('role.addNewRole') }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="roleName" class="block text-sm font-medium text-gray-700 mb-1">{{ t('role.name') }}</label>
              <input type="text" id="roleName" v-model="formData.name"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :placeholder="t('role.pleaseEnterRoleName')" required>
            </div>
            <div class="mb-6">
              <label for="description" class="block text-sm font-medium text-gray-700 mb-1">{{ t('role.description')
                }}</label>
              <textarea id="description" v-model="formData.description"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                rows="2" :placeholder="t('role.pleaseEnterRoleDescription')"></textarea>
            </div>

            <!-- 关联策略 -->
            <div class="mb-6">
              <label class="block text-sm font-medium text-gray-700 mb-2">{{ t('role.policyAssociation') }}</label>
              <div
                class="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-60 overflow-y-auto p-2 border border-gray-200 rounded-lg">
                <label v-for="policy in policiesList" :key="policy.id"
                  class="flex items-center p-2 hover:bg-gray-50 rounded cursor-pointer">
                  <input type="checkbox" :value="policy.id" v-model="formData.policyIds"
                    class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <span class="ml-2 text-sm text-gray-700">{{ policy.name }}</span>
                  <span v-if="policy.description" class="ml-2 text-xs text-gray-500">({{ policy.description }})</span>
                </label>
              </div>
              <div v-if="policiesList.length === 0" class="text-sm text-gray-500 p-4 text-center">
                {{ t('role.noAvailablePolicies') }}
              </div>
            </div>


          </form>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('role.cancel') }}
          </button>
          <button @click="handleSubmit"
            class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ isEditMode ? t('role.save') : t('role.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 角色详情对话框 -->
    <div v-if="detailsVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('role.roleDetails') }}</h3>
          <button @click="closeDetails" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">{{ t('role.name') }}</h4>
            <p class="font-medium text-gray-900">{{ currentRole.name }}</p>
          </div>
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">{{ t('role.description') }}</h4>
            <p class="text-gray-700">{{ currentRole.description || t('role.noDescription') }}</p>
          </div>
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">{{ t('role.associatedPolicies') }}</h4>
            <div v-if="currentRole.policies && currentRole.policies.length > 0">
              <span v-for="policy in currentRole.policies" :key="policy.id"
                class="inline-block px-2 py-1 bg-green-100 text-green-800 rounded mr-2 mb-2">
                {{ policy.name }}
              </span>
            </div>
            <span v-else class="text-gray-400">{{ t('role.noAssociatedPolicies') }}</span>
          </div>

          <div class="mb-2">
            <h4 class="text-sm font-medium text-gray-500">{{ t('role.creationTime') }}</h4>
            <p class="text-gray-700">{{ formatDate(currentRole.createAt || currentRole.createdAt) }}</p>
          </div>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDetails"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('common.close') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('role.confirmDeleteTitle') }}</h3>
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors"
            aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('role.confirmDeleteMessage', { name: currentRole.name }) }}</p>
          <p class="text-gray-700">{{ t('role.confirmDeleteDescription') }}</p>
          <p v-if="policiesInCurrentRole > 0" class="text-sm text-red-500 mt-2">
            {{ t('role.confirmDeletePolicyWarning', { count: policiesInCurrentRole }) }}
          </p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('role.cancel') }}
          </button>
          <button @click="confirmDeleteRole"
            class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('role.deleteRole') }}
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
import { listrole, getrole, createrole, setrole, delrole, listpolicy } from '../api/admin.js';

// 获取翻译函数
const { t } = useI18n();

// 角色和策略数据
const rolesList = ref([]);
const policiesList = ref([]);

// 搜索关键字
const searchKeyword = ref('');

// 对话框状态
const dialogVisible = ref(false);
const detailsVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditMode = ref(false);
const currentRoleId = ref(null);
const currentRole = ref({});
const policiesInCurrentRole = ref(0);

// 表单数据
const formData = ref({
  name: '',
  description: '',
  policyIds: []
});

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 过滤角色列表
const filteredRoles = computed(() => {
  if (!searchKeyword.value) {
    return rolesList.value;
  }
  return rolesList.value.filter(role =>
    role.name.toLowerCase().includes(searchKeyword.value.toLowerCase()) ||
    (role.description && role.description.toLowerCase().includes(searchKeyword.value.toLowerCase()))
  );
});

// 从API获取角色列表
const loadRolesFromAPI = async () => {
  try {
    const response = await listrole();
    if (response.code === 0) {
      // 处理API返回的数据，确保每个角色都有policies字段
      rolesList.value = (response.data || []).map(role => ({
        ...role,
        // 转换attachedPolicies为policies对象数组，或初始化为空数组
        policies: role.attachedPolicies ? role.attachedPolicies.map(policyName => ({
          id: policyName, // 使用策略名称作为临时ID
          name: policyName
        })) : []
      }));
    } else {
      showToastMessage(response.msg || t('common.networkErrorPleaseRetry'), 'error');
    }
  } catch (error) {
    console.error('获取角色列表异常:', error);
    showToastMessage(t('common.networkErrorPleaseRetry'), 'error');
  }
};

// 从API获取策略列表
const loadPoliciesFromAPI = async () => {
  try {
    const response = await listpolicy();
    if (response.code === 0) {
      // 确保每个策略都有唯一的ID
      policiesList.value = (response.data || []).map(policy => ({
        ...policy,
        // 如果没有ID或ID不唯一，使用name作为ID或生成唯一ID
        id: policy.id || policy.name || `policy_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
      }));
    } else {
      showToastMessage(response.msg || t('common.networkErrorPleaseRetry'), 'error');
    }
  } catch (error) {
    console.error('获取策略列表异常:', error);
    showToastMessage(t('common.networkErrorPleaseRetry'), 'error');
  }
};

// 获取角色详情（包含策略信息）
const getRoleDetails = async (roleName) => {
  try {
    const response = await getrole({ name: roleName });
    if (response.code === 0) {
      const role = response.data;
      // 将策略名称转换为策略对象引用
      role.policies = policiesList.value.filter(policy => 
        role.attachedPolicies.includes(policy.name)
      );
      return role;
    } else {
      showToastMessage(response.msg || t('common.networkErrorPleaseRetry'), 'error');
      return null;
    }
  } catch (error) {
    console.error('获取角色详情异常:', error);
    showToastMessage(t('common.networkErrorPleaseRetry'), 'error');
    return null;
  }
};

// 显示添加角色对话框
const showAddRoleDialog = () => {
  isEditMode.value = false;
  formData.value = {
    name: '',
    description: '',
    policyIds: []
  };
  dialogVisible.value = true;
};

// 显示编辑角色对话框
const showEditRoleDialog = (role) => {
  isEditMode.value = true;
  currentRoleId.value = role.name; // 使用角色名称作为ID
  formData.value = {
    name: role.name,
    description: role.description,
    policyIds: role.policies ? role.policies.map(policy => policy.id) : []
  };
  dialogVisible.value = true;
};

// 显示角色详情
const showRoleDetails = async (role) => {
  const roleWithDetails = await getRoleDetails(role.name);
  if (roleWithDetails) {
    currentRole.value = roleWithDetails;
    detailsVisible.value = true;
  }
};

// 关闭对话框
const closeDialog = () => {
  dialogVisible.value = false;
  currentRoleId.value = null;
};

// 关闭详情对话框
const closeDetails = () => {
  detailsVisible.value = false;
  currentRole.value = {};
};

// 关闭删除对话框
const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentRoleId.value = null;
  policiesInCurrentRole.value = 0;
};

// 提交表单
const handleSubmit = async () => {
  if (!formData.value.name) {
    showToastMessage(t('role.pleaseEnterRoleName'), 'error');
    return;
  }

  // 准备API请求参数
  const requestData = {
    name: formData.value.name,
    desc: formData.value.description,
    attachPolicies: policiesList.value
      .filter(policy => formData.value.policyIds.includes(policy.id))
      .map(policy => policy.name)
  };

  try {
    if (isEditMode.value) {
      // 编辑角色
      const response = await setrole(requestData);
      if (response.code === 0) {
        showToastMessage(t('role.updatedSuccess'), 'success');
        // 重新加载角色列表以更新数据
        await loadRolesFromAPI();
      } else {
        showToastMessage(response.msg || '更新角色失败', 'error');
      }
    } else {
      // 添加角色
      const response = await createrole(requestData);
      if (response.code === 0) {
        showToastMessage(t('role.createdSuccess'), 'success');
        // 重新加载角色列表以显示新创建的角色
        await loadRolesFromAPI();
      } else {
        showToastMessage(response.msg || '创建角色失败', 'error');
      }
    }
    closeDialog();
  } catch (error) {
      console.error('提交表单异常:', error);
      showToastMessage(t('common.networkErrorPleaseRetry'), 'error');
    }
};

// 处理删除角色
const handleDeleteRole = (roleName) => {
  currentRoleId.value = roleName;
  const role = rolesList.value.find(r => r.name === roleName);
  if (role) {
    policiesInCurrentRole.value = role.policies ? role.policies.length : 0;
  }
  deleteDialogVisible.value = true;
};

// 确认删除角色
const confirmDeleteRole = async () => {
  try {
    const response = await delrole({ name: currentRoleId.value });
    if (response.code === 0) {
      showToastMessage(t('role.deletedSuccess'), 'success');
      // 重新加载角色列表以更新数据
      await loadRolesFromAPI();
    } else {
      showToastMessage(response.msg || '删除角色失败', 'error');
    }
  } catch (error) {
    console.error('删除角色异常:', error);
    showToastMessage(t('common.networkErrorPleaseRetry'), 'error');
  }
  closeDeleteDialog();
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

// 格式化日期
const formatDate = (date) => {
  if (!date) return '';
  
  let dateObj;
  try {
    // 尝试创建日期对象
    dateObj = new Date(date);
  } catch (error) {
    console.error('日期格式化错误:', error);
    return t('role.invalidDate');
  }
  
  // 检查日期是否有效或是否为默认的0001-01-01日期
  if (isNaN(dateObj.getTime()) || 
      dateObj.toISOString() === '0001-01-01T00:00:00.000Z' ||
      dateObj.getTime() < 10000000000) { // 过滤极早的日期
    return t('role.noCreationTime') || '暂无创建时间';
  }
  
  // 格式化日期为可读格式
  const year = dateObj.getFullYear();
  const month = String(dateObj.getMonth() + 1).padStart(2, '0');
  const day = String(dateObj.getDate()).padStart(2, '0');
  const hours = String(dateObj.getHours()).padStart(2, '0');
  const minutes = String(dateObj.getMinutes()).padStart(2, '0');
  const seconds = String(dateObj.getSeconds()).padStart(2, '0');
  
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};

// 组件挂载时加载数据
onMounted(async () => {
  await Promise.all([
    loadRolesFromAPI(),
    loadPoliciesFromAPI()
  ]);
});
</script>

<style scoped>
.roles-container {
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
  .roles-container {
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

  .max-w-3xl,
  .max-w-md {
    margin: 1rem;
    width: calc(100% - 2rem);
  }

  .flex-col {
    max-height: calc(100vh - 2rem);
  }
}
</style>