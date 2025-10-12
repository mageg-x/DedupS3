<template>
  <div class="users-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">{{ t('user.pageTitle') }}</h1>
      <button @click="showAddUserDialog"
        class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
        <i class="fas fa-plus"></i>
        <span>{{ t('user.addUser') }}</span>
      </button>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" v-model="searchKeyword" :placeholder="t('user.searchPlaceholder')"
          class="w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 用户列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.userName') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.associatedRole') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.associatedPolicy') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.associatedGroup') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.status') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.creationTime') }}</th>
              <th class="px-6 py-3 text-right text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('user.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="user in filteredUsers" :key="user.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div
                    class="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center text-blue-600 font-medium">
                    {{ user.username.charAt(0).toUpperCase() }}
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ user.username }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <div v-if="user.roles && user.roles.length > 0">
                  <span v-for="role in user.roles" :key="role.id"
                    class="inline-block text-sm px-2 py-1 bg-orange-100 text-orange-800 rounded mr-2 mb-2">
                    {{ role.name }}
                  </span>
                </div>
                <span v-else class="text-gray-400 text-sm">{{ t('user.noAssociatedRoles') }}</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <div v-if="user.policies && user.policies.length > 0">
                  <span v-for="policy in user.policies" :key="policy.id"
                    class="inline-block text-sm px-2 py-1 bg-blue-100 text-blue-800 rounded mr-2 mb-2">
                    {{ policy.name }}
                  </span>
                </div>
                <span v-else class="text-gray-400 text-sm">{{ t('user.noAssociatedPolicies') }}</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                <div v-if="user.groups && user.groups.length > 0">
                  <span v-for="group in user.groups" :key="group.id"
                    class="inline-block text-sm px-2 py-1 bg-gray-100 text-gray-800 rounded mr-2 mb-2">
                    {{ group.name }}
                  </span>
                </div>
                <span v-else class="text-gray-400 text-sm">{{ t('user.noAssociatedGroups') }}</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span
                  :class="['px-2 py-1 text-sm rounded-full', user.active ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800']">
                  {{ user.active ? t('user.enabled') : t('user.disabled') }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(user.createdAt) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showEditUserDialog(user)"
                  class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-edit mr-1"></i>{{ t('user.edit') }}
                </button>
                <button @click="handleDeleteUser(user.username)"
                  class="text-red-600 hover:text-red-900 transition-colors">
                  <i class="fas fa-trash-alt mr-1"></i>{{ t('user.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- 空状态 -->
      <div v-if="filteredUsers.length === 0" class="py-12 text-center">
      <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('user.noUsers') }}</h3>
        <p class="text-gray-500 mb-6">{{ t('user.clickAddUser') }}</p>
      </div>
    </div>

    <!-- 添加/编辑用户对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn flex flex-col max-h-[90vh]">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? t('user.editUser') : t('user.addNewUser') }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 overflow-y-auto flex-grow max-h-[60vh]">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="username" class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.userName')
              }}</label>
              <input type="text" id="username" v-model="formData.username"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :disabled="isEditMode" required>
            </div>
            <div class="mb-4">
              <label for="password" class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.password')
              }}</label>
              <input type="password" id="password" v-model="formData.password"
                class="w-full px-4 py-2 text-sm  border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :placeholder="isEditMode ? t('user.keepEmptyPassword') : ''">
              <span v-if="isEditMode" class="text-xs text-gray-500">{{ t('user.keepEmptyPassword') }}</span>
            </div>

            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.associatedGroup') }}</label>
              <div class="space-y-2">
                <div v-for="group in groupsList" :key="group.id" class="flex items-center">
                  <input type="checkbox" :id="`group-${group.id}`" :checked="formData.groups.includes(group.id)"
                    @change="toggleGroup(group.id)"
                    class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`group-${group.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ group.name }}
                  </label>
                </div>
                <div v-if="groupsList.length === 0" class="text-sm text-gray-500 py-2">
                  {{ t('user.noAvailableGroups') }}
                </div>
              </div>
            </div>
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.associatedRole') }}</label>
              <div class="space-y-2">
                <div v-for="role in rolesList" :key="role.id" class="flex items-center">
                  <input type="checkbox" :id="`role-${role.id}`" :checked="formData.roles.includes(role.id)"
                    @change="toggleRole(role.id)"
                    class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`role-${role.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ role.name }}
                    <span v-if="role.description" class="text-gray-500">({{ role.description }})</span>
                  </label>
                </div>
                <div v-if="rolesList.length === 0" class="text-sm text-gray-500 py-2">
                  {{ t('user.noAvailableRoles') }}
                </div>
              </div>
            </div>
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.associatedPolicy') }}</label>
              <div class="space-y-2">
                <div v-for="policy in policiesList" :key="policy.id" class="flex items-center">
                  <input type="checkbox" :id="`policy-${policy.id}`" :checked="formData.policies.includes(policy.id)"
                    @change="togglePolicy(policy.id)"
                    class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`policy-${policy.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ policy.name }}
                    <span v-if="policy.description" class="text-gray-500">({{ policy.description }})</span>
                  </label>
                </div>
                <div v-if="policiesList.length === 0" class="text-sm text-gray-500 py-2">
                  {{ t('user.noAvailablePolicies') }}
                </div>
              </div>
            </div>
            <div class="mb-4">
              <label class="flex items-center">
                <input type="checkbox" v-model="formData.active"
                  class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                <span class="ml-2 text-sm text-gray-700">{{ t('user.enableUser') }}</span>
              </label>
            </div>
          </form>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('user.cancel') }}
          </button>
          <button @click="handleSubmit"
            class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ isEditMode ? t('user.update') : t('user.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('user.confirmDelete') }}</h3>
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors"
            aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('user.confirmDeleteMessage', { name: currentUserId }) }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('user.cancel') }}
          </button>
          <button @click="confirmDeleteUser"
            class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('user.confirmDelete') }}
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
import { ElMessage } from 'element-plus';
import { listuser, listgroup, listrole, listpolicy, createuser, setuser, deluser } from '@/api/admin';

// 获取翻译函数
const { t } = useI18n();

// 数据
const usersList = ref([]);
const groupsList = ref([]);
const rolesList = ref([]);
const policiesList = ref([]);

// 搜索关键字
const searchKeyword = ref('');

// 对话框状态
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditMode = ref(false);
const currentUserId = ref(null);

// 表单数据
const formData = ref({
  username: '',
  password: '',
  groups: [],
  roles: [],
  policies: [],
  active: true
});

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 计算属性
const filteredUsers = computed(() => {
  if (!searchKeyword.value) {
    return usersList.value;
  }
  return usersList.value.filter(user =>
    user.username.toLowerCase().includes(searchKeyword.value.toLowerCase())
  );
});

// 加载数据
onMounted(async () => {
  await Promise.all([
    loadGroups(),
    loadRoles(),
    loadPolicies(),
    loadUsers()
  ]);
});

// 加载用户组数据
const loadGroups = async () => {
  try {
    const response = await listgroup();
    if (response.code === 0 && response.data) {
      groupsList.value = response.data.map(group => ({
        id: group.name, // 使用name作为唯一标识符
        name: group.name,
        description: group.description || ''
      }));
    }
  } catch (error) {
    console.error('获取用户组列表失败:', error);
    ElMessage.error(t('user.fetchGroupsFailed'));
  }
};

// 加载角色数据
const loadRoles = async () => {
  try {
    const response = await listrole();
    if (response.code === 0 && response.data) {
      rolesList.value = response.data.map(role => ({
        id: role.name, // 使用name作为唯一标识符
        name: role.name,
        description: role.description || ''
      }));
    }
  } catch (error) {
    console.error('获取角色列表失败:', error);
    ElMessage.error(t('user.fetchRolesFailed'));
  }
};

// 加载策略数据
const loadPolicies = async () => {
  try {
    const response = await listpolicy();
    if (response.code === 0 && response.data) {
      policiesList.value = response.data.map(policy => ({
        id: policy.name, // 使用name作为唯一标识符
        name: policy.name,
        description: policy.description || ''
      }));
    }
  } catch (error) {
    console.error('获取策略列表失败:', error);
    ElMessage.error(t('user.fetchPoliciesFailed'));
  }
};

// 从API加载用户数据
const loadUsers = async () => {
  try {
    // 调用API获取用户列表
    const response = await listuser();

    if (response.code === 0 && response.data) {
      // 清空旧数据
      usersList.value = [];

      // 处理API返回的数据
      response.data.forEach(apiUser => {
        // 转换为前端需要的用户格式
        usersList.value.push({
          id: apiUser.username, // 使用username作为唯一标识符
          username: apiUser.username,
          groups: apiUser.group?.map(groupName => ({
            id: groupName,
            name: groupName,
            description: ''
          })) || [],
          roles: apiUser.role?.map(roleName => ({
            id: roleName,
            name: roleName,
            description: ''
          })) || [],
          policies: apiUser.attachPolicies?.map(policyName => ({
            id: policyName,
            name: policyName,
            description: ''
          })) || [],
          active: apiUser.enabled,
          createdAt: new Date(apiUser.createdAt)
        });
      });
    } else {
      console.warn('API返回的数据无效');
    }
  } catch (error) {
    console.error('获取用户列表失败:', error);
    ElMessage.error(t('user.fetchUsersFailed'));
  }
};

// 对话框控制
const showAddUserDialog = () => {
  resetForm();
  isEditMode.value = false;
  dialogVisible.value = true;
};

const showEditUserDialog = (user) => {
  resetForm();
  isEditMode.value = true;
  currentUserId.value = user.username;
  formData.value = {
    username: user.username,
    password: '',
    groups: user.groups.map(group => group.id),
    roles: user.roles ? user.roles.map(role => role.id) : [],
    policies: user.policies ? user.policies.map(policy => policy.id) : [],
    active: user.active
  };
  dialogVisible.value = true;
};

const closeDialog = () => {
  dialogVisible.value = false;
  currentUserId.value = null;
};

const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentUserId.value = null;
};

// 表单控制
const resetForm = () => {
  formData.value = {
    username: '',
    password: '',
    groups: [],
    roles: [],
    policies: [],
    active: true
  };
};

const toggleGroup = (groupId) => {
  const index = formData.value.groups.indexOf(groupId);
  if (index > -1) {
    formData.value.groups.splice(index, 1);
  } else {
    formData.value.groups.push(groupId);
  }
};

const toggleRole = (roleId) => {
  const index = formData.value.roles.indexOf(roleId);
  if (index > -1) {
    formData.value.roles.splice(index, 1);
  } else {
    formData.value.roles.push(roleId);
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

// 提交表单
const handleSubmit = async () => {
  if (!formData.value.username) {
    showToastMessage(t('user.usernameRequired'), 'error');
    return;
  }

  if (!isEditMode.value && !formData.value.password) {
    showToastMessage(t('user.passwordRequired'), 'error');
    return;
  }

  try {
    if (isEditMode.value) {
      // 编辑用户
      const updateData = {
        username: formData.value.username,
        groups: formData.value.groups,
        roles: formData.value.roles,
        attachPolicies: formData.value.policies,
        enabled: formData.value.active
      };

      if (formData.value.password) {
        updateData.password = formData.value.password;
      }

      const response = await setuser(updateData);
      if (response.code === 0) {
        showToastMessage(t('user.userUpdated'), 'success');
        await loadUsers(); // 重新加载用户列表
        closeDialog();
      } else {
        showToastMessage(response.msg || t('user.updateUserFailed'), 'error');
      }
    } else {
      // 添加用户
      const createData = {
        username: formData.value.username,
        password: formData.value.password,
        groups: formData.value.groups,
        roles: formData.value.roles,
        attachPolicies: formData.value.policies,
        enabled: formData.value.active
      };

      const response = await createuser(createData);
      if (response.code === 0) {
        showToastMessage(t('user.userCreated'), 'success');
        await loadUsers(); // 重新加载用户列表
        closeDialog();
      } else {
        showToastMessage(response.msg || t('user.createUserFailed'), 'error');
      }
    }
  } catch (error) {
    console.error('操作用户失败:', error);
    showToastMessage(t('user.userOperationFailed'), 'error');
  }
};

// 删除用户
const handleDeleteUser = (username) => {
  currentUserId.value = username;
  deleteDialogVisible.value = true;
};

const confirmDeleteUser = async () => {
  try {
    const response = await deluser({ username: currentUserId.value });
    if (response.code === 0) {
      showToastMessage(t('user.userDeleted'), 'success');
      await loadUsers(); // 重新加载用户列表
      closeDeleteDialog();
    } else {
      showToastMessage(response.msg || t('user.deleteUserFailed'), 'error');
    }
  } catch (error) {
    console.error('删除用户失败:', error);
    showToastMessage(t('user.deleteUserFailedRetry'), 'error');
  }
};

// 工具函数
const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  // 3秒后自动隐藏
  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

const formatDate = (date) => {
  if (!(date instanceof Date)) {
    date = new Date(date);
  }
  return date.toLocaleString('zh-CN');
};
</script>

<style scoped>
.users-container {
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
  .users-container {
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
}
</style>