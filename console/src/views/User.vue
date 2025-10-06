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
        <input type="text" 
               v-model="searchKeyword" 
               :placeholder="t('user.searchPlaceholder')" 
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
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.userName') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.associatedRole') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.associatedPolicy') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.associatedGroup') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.status') }}</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.creationTime') }}</th>
              <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">{{ t('user.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="user in filteredUsers" :key="user.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div class="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center text-blue-600 font-medium">
                    {{ user.username.charAt(0).toUpperCase() }}
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ user.username }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <div v-if="user.roles && user.roles.length > 0">
                  <span v-for="role in user.roles" :key="role.id" class="inline-block px-2 py-1 bg-orange-100 text-orange-800 rounded mr-2 mb-2">
                    {{ role.name }}
                  </span>
                </div>
                <span v-else class="text-gray-400">{{ t('user.noAssociatedRoles') }}</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <div v-if="user.policies && user.policies.length > 0">
                  <span v-for="policy in user.policies" :key="policy.id" class="inline-block px-2 py-1 bg-blue-100 text-blue-800 rounded mr-2 mb-2">
                    {{ policy.name }}
                  </span>
                </div>
                <span v-else class="text-gray-400">{{ t('user.noAssociatedPolicies') }}</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                <div v-if="user.groups && user.groups.length > 0">
                  <span v-for="group in user.groups" :key="group.id" class="inline-block px-2 py-1 bg-gray-100 text-gray-800 rounded mr-2 mb-2">
                    {{ group.name }}
                  </span>
                </div>
                <span v-else class="text-gray-400">{{ t('user.noAssociatedGroups') }}</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="['px-2 py-1 text-xs rounded-full', user.active ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800']">
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
                <button @click="handleDeleteUser(user.id)"
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
        <div class="text-gray-400 mb-4">
          <i class="fas fa-user-slash text-4xl"></i>
        </div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('user.noUsers') }}</h3>
          <p class="text-gray-500 mb-6">{{ t('user.clickAddUser') }}</p>
      </div>
    </div>

    <!-- 添加/编辑用户对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
          <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn flex flex-col max-h-[90vh]">
            <div class="p-5 border-b border-gray-100 flex items-center justify-between">
              <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? t('user.editUser') : t('user.addNewUser') }}</h3>
              <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
                <i class="fas fa-times"></i>
              </button>
            </div>
            <div class="p-5 overflow-y-auto flex-grow max-h-[60vh]">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="username" class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.userName') }}</label>
              <input type="text" id="username" v-model="formData.username" 
                     class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                     :disabled="isEditMode" required>
            </div>
            <div v-if="!isEditMode" class="mb-4">
              <label for="password" class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.password') }}</label>
              <input type="password" id="password" v-model="formData.password" 
                     class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                     required>
            </div>

            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.associatedGroup') }}</label>
              <div class="space-y-2">
                <div v-for="group in groupsList" :key="group.id" class="flex items-center">
                  <input type="checkbox" 
                         :id="`group-${group.id}`" 
                         :checked="formData.groups.includes(group.id)"
                         @change="handleGroupToggle(group.id)" 
                         class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`group-${group.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ group.name }}
                  </label>
                </div>
              </div>
            </div>
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-1">{{ t('user.associatedRole') }}</label>
              <div class="space-y-2">
                <div v-for="role in rolesList" :key="role.id" class="flex items-center">
                  <input type="checkbox" 
                         :id="`role-${role.id}`" 
                         :checked="formData.roles.includes(role.id)"
                         @change="handleRoleToggle(role.id)" 
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
                  <input type="checkbox" 
                         :id="`policy-${policy.id}`" 
                         :checked="formData.policies.includes(policy.id)"
                         @change="handlePolicyToggle(policy.id)" 
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
                <input type="checkbox" v-model="formData.active" class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                <span class="ml-2 text-sm text-gray-700">{{ t('user.enableUser') }}</span>
              </label>
            </div>
          </form>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
                {{ t('user.cancel') }}
              </button>
              <button @click="handleSubmit" class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
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
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('user.confirmDeleteMessage') }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
                {{ t('user.cancel') }}
              </button>
              <button @click="confirmDeleteUser" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
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
import { ref, computed, onMounted, watch } from 'vue';
import { useI18n } from 'vue-i18n';

// 获取翻译函数
const { t } = useI18n();

// 模拟数据
const usersList = ref([]);
const groupsList = ref([]);
const rolesList = ref([]);
const policiesList = ref([]);
let nextUserId = 1;

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

// 过滤用户列表
const filteredUsers = computed(() => {
  if (!searchKeyword.value) {
    return usersList.value;
  }
  return usersList.value.filter(user => 
    user.username.toLowerCase().includes(searchKeyword.value.toLowerCase())
  );
});

// 加载模拟数据
  const loadMockData = () => {
    // 模拟用户组数据
    groupsList.value = [
      { id: 1, name: '开发组', description: '开发人员' },
      { id: 2, name: '测试组', description: '测试人员' },
      { id: 3, name: '管理员组', description: '系统管理员' }
    ];

    // 模拟角色数据
    rolesList.value = [
      { id: 1, name: '系统管理员', description: '系统全部权限' },
      { id: 2, name: '数据分析师', description: '数据查看和分析权限' },
      { id: 3, name: '内容审核员', description: '内容审核权限' }
    ];

    // 模拟策略数据
    policiesList.value = [
      { id: 1, name: '账户管理权限', description: '管理所有用户' },
      { id: 2, name: '数据查看权限', description: '查看所有数据' },
      { id: 3, name: '内容编辑权限', description: '编辑系统内容' },
      { id: 4, name: '日志查看权限', description: '查看系统日志' }
    ];

    // 模拟用户数据
    usersList.value = [
      {
        id: 1,
        username: 'admin',
        password: '******', 
        groups: [groupsList.value[2]], 
        roles: [rolesList.value[0]],
        policies: [policiesList.value[0]],
        active: true, 
        createdAt: new Date('2023-01-15T10:00:00')
      },
      {
        id: 2,
        username: 'developer',
        password: '******', 
        groups: [groupsList.value[0], groupsList.value[1]], 
        roles: [rolesList.value[1]],
        policies: [policiesList.value[1], policiesList.value[3]],
        active: true, 
        createdAt: new Date('2023-01-20T14:30:00')
      },
      {
        id: 3,
        username: 'tester',
        password: '******', 
        groups: [groupsList.value[1]], 
        roles: [],
        policies: [policiesList.value[2]],
        active: true, 
        createdAt: new Date('2023-01-25T09:15:00')
      }
    ];

  // 移除本地存储中可能存在的旧数据以避免冲突
  localStorage.removeItem('usersList');

  nextUserId = 4;
};

// 显示添加用户对话框
  const showAddUserDialog = () => {
    isEditMode.value = false;
    formData.value = {
      username: '',
      password: '',
      groups: [],
      roles: [],
      policies: [],
      active: true
    };
    dialogVisible.value = true;
  };

// 显示编辑用户对话框
  const showEditUserDialog = (user) => {
    isEditMode.value = true;
    currentUserId.value = user.id;
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

// 关闭对话框
const closeDialog = () => {
  dialogVisible.value = false;
  currentUserId.value = null;
};

// 关闭删除对话框
const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentUserId.value = null;
};

// 处理用户组选择
const handleGroupToggle = (groupId) => {
  const index = formData.value.groups.indexOf(groupId);
  if (index > -1) {
    formData.value.groups.splice(index, 1);
  } else {
    formData.value.groups.push(groupId);
  }
};

// 处理角色选择
  const handleRoleToggle = (roleId) => {
    const index = formData.value.roles.indexOf(roleId);
    if (index > -1) {
      formData.value.roles.splice(index, 1);
    } else {
      formData.value.roles.push(roleId);
    }
  };

  // 处理策略选择
  const handlePolicyToggle = (policyId) => {
    const index = formData.value.policies.indexOf(policyId);
    if (index > -1) {
      formData.value.policies.splice(index, 1);
    } else {
      formData.value.policies.push(policyId);
    }
  };

// 提交表单
const handleSubmit = () => {
  if (!formData.value.username) {
    showToastMessage('用户名不能为空', 'error');
    return;
  }

  if (!isEditMode && !formData.value.password) {
    showToastMessage('密码不能为空', 'error');
    return;
  }

  // 获取完整的用户组信息
    const selectedGroups = groupsList.value.filter(group => 
      formData.value.groups.includes(group.id)
    );
    
    // 获取完整的角色信息
    const selectedRoles = rolesList.value.filter(role => 
      formData.value.roles.includes(role.id)
    );
    
    // 获取完整的策略信息
    const selectedPolicies = policiesList.value.filter(policy => 
      formData.value.policies.includes(policy.id)
    );

  if (isEditMode) {
    // 编辑用户
    const index = usersList.value.findIndex(user => user.id === currentUserId.value);
    if (index !== -1) {
      usersList.value[index] = {
        ...usersList.value[index],
        groups: selectedGroups,
        roles: selectedRoles,
        policies: selectedPolicies,
        active: formData.value.active
      };
      saveUsers();
      showToastMessage('用户已更新', 'success');
    }
  } else {
    // 添加用户
    const newUser = {
        id: nextUserId++,
        username: formData.value.username,
        password: '******', // 实际项目中应该加密存储
        groups: selectedGroups,
        roles: selectedRoles,
        policies: selectedPolicies,
        active: formData.value.active,
        createdAt: new Date()
      };
    usersList.value.push(newUser);
    saveUsers();
    showToastMessage('用户已创建', 'success');
  }

  closeDialog();
};

// 处理删除用户
const handleDeleteUser = (userId) => {
  currentUserId.value = userId;
  deleteDialogVisible.value = true;
};

// 确认删除用户
const confirmDeleteUser = () => {
  const index = usersList.value.findIndex(user => user.id === currentUserId.value);
  if (index !== -1) {
    usersList.value.splice(index, 1);
    saveUsers();
    showToastMessage('用户已删除', 'success');
  }
  closeDeleteDialog();
};

// 保存用户数据到本地存储
  const saveUsers = () => {
    try {
      localStorage.setItem('usersList', JSON.stringify(usersList.value));
      localStorage.setItem('rolesList', JSON.stringify(rolesList.value));
      localStorage.setItem('policiesList', JSON.stringify(policiesList.value));
    } catch (error) {
      console.error('保存用户列表失败:', error);
    }
  };

// 从本地存储加载用户数据
const loadUsers = () => {
  try {
      const savedUsers = localStorage.getItem('usersList');
      const savedRoles = localStorage.getItem('rolesList');
      const savedPolicies = localStorage.getItem('policiesList');
      
      if (savedPolicies) {
        policiesList.value = JSON.parse(savedPolicies);
      }
      
      if (savedRoles) {
        rolesList.value = JSON.parse(savedRoles);
      }
    
    if (savedUsers) {
      usersList.value = JSON.parse(savedUsers);
      // 恢复日期对象
      usersList.value.forEach(user => {
        user.createdAt = new Date(user.createdAt);
        // 恢复用户组引用
        user.groups = groupsList.value.filter(group => 
          user.groups.some(g => g.id === group.id || g === group.id)
        );
        // 恢复角色引用
        if (user.roles) {
          user.roles = rolesList.value.filter(role => 
            user.roles.some(r => r.id === role.id || r === role.id)
          );
        } else {
          user.roles = [];
        }
        
        // 恢复策略引用
        if (user.policies) {
          user.policies = policiesList.value.filter(policy => 
            user.policies.some(p => p.id === policy.id || p === policy.id)
          );
        } else {
          user.policies = [];
        }
      });
      // 更新下一个用户ID
      if (usersList.value.length > 0) {
        nextUserId = Math.max(...usersList.value.map(user => user.id)) + 1;
      }
    }
  } catch (error) {
    console.error('加载用户列表失败:', error);
    loadMockData(); // 加载失败时使用模拟数据
  }
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
  if (!(date instanceof Date)) {
    date = new Date(date);
  }
  return date.toLocaleString('zh-CN');
};

// 组件挂载时加载数据
onMounted(() => {
  loadMockData();
  loadUsers();
});
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
  
  th, td {
    padding: 0.75rem !important;
  }
  
  .dialog-container {
    margin: 1rem;
    width: calc(100% - 2rem);
  }
}
</style>