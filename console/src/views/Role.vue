<template>
  <div class="roles-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">角色管理</h1>
      <button @click="showAddRoleDialog" 
              class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
        <i class="fas fa-plus"></i>
        <span>添加角色</span>
      </button>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" 
               v-model="searchKeyword" 
               placeholder="搜索角色名称或描述..." 
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
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">名称</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">描述</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">关联策略</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">关联用户</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">创建时间</th>
              <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">操作</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="role in filteredRoles" :key="role.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div class="w-10 h-10 rounded-full bg-orange-100 flex items-center justify-center text-orange-600 font-medium">
                    <i class="fas fa-user-tag"></i>
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ role.name }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 text-sm text-gray-500">
                {{ role.description || '无描述' }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-xs font-medium rounded-full bg-green-100 text-green-800">
                  {{ role.policies ? role.policies.length : 0 }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-xs font-medium rounded-full bg-blue-100 text-blue-800">
                  {{ role.users ? role.users.length : 0 }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(role.createdAt) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showRoleDetails(role)" 
                        class="text-green-600 hover:text-green-900 transition-colors mr-3">
                  <i class="fas fa-eye mr-1"></i>查看
                </button>
                <button @click="showEditRoleDialog(role)" 
                        class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-edit mr-1"></i>编辑
                </button>
                <button @click="handleDeleteRole(role.id)" 
                        class="text-red-600 hover:text-red-900 transition-colors">
                  <i class="fas fa-trash-alt mr-1"></i>删除
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
        <h3 class="text-lg font-medium text-gray-900 mb-2">暂无角色</h3>
        <p class="text-gray-500 mb-6">点击上方"添加角色"按钮创建第一个角色</p>
      </div>
    </div>

    <!-- 添加/编辑角色对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? '编辑角色' : '添加角色' }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="roleName" class="block text-sm font-medium text-gray-700 mb-1">角色名称</label>
              <input type="text" id="roleName" v-model="formData.name" 
                     class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                     required>
            </div>
            <div class="mb-6">
              <label for="description" class="block text-sm font-medium text-gray-700 mb-1">描述</label>
              <textarea id="description" v-model="formData.description" 
                        class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                        rows="2"></textarea>
            </div>
            
            <!-- 关联策略 -->
            <div class="mb-6">
              <label class="block text-sm font-medium text-gray-700 mb-2">关联策略</label>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-60 overflow-y-auto p-2 border border-gray-200 rounded-lg">
                <label v-for="policy in policiesList" :key="policy.id" class="flex items-center p-2 hover:bg-gray-50 rounded cursor-pointer">
                  <input type="checkbox" :value="policy.id" v-model="formData.policyIds" 
                         class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <span class="ml-2 text-sm text-gray-700">{{ policy.name }}</span>
                  <span v-if="policy.description" class="ml-2 text-xs text-gray-500">({{ policy.description }})</span>
                </label>
              </div>
              <div v-if="policiesList.length === 0" class="text-sm text-gray-500 p-4 text-center">
                暂无可用策略，请先创建策略
              </div>
            </div>
            
            <!-- 关联用户 -->
            <div class="mb-6">
              <label class="block text-sm font-medium text-gray-700 mb-2">关联用户</label>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-60 overflow-y-auto p-2 border border-gray-200 rounded-lg">
                <label v-for="user in usersList" :key="user.id" class="flex items-center p-2 hover:bg-gray-50 rounded cursor-pointer">
                  <input type="checkbox" :value="user.id" v-model="formData.userIds" 
                         class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <span class="ml-2 text-sm text-gray-700">{{ user.username }}</span>
                  <span class="ml-2 text-xs text-gray-500">({{ user.role }})</span>
                </label>
              </div>
              <div v-if="usersList.length === 0" class="text-sm text-gray-500 p-4 text-center">
                暂无可用用户，请先创建用户
              </div>
            </div>
          </form>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            取消
          </button>
          <button @click="handleSubmit" class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ isEditMode ? '更新' : '创建' }}
          </button>
        </div>
      </div>
    </div>

    <!-- 角色详情对话框 -->
    <div v-if="detailsVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">角色详情</h3>
          <button @click="closeDetails" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">名称</h4>
            <p class="font-medium text-gray-900">{{ currentRole.name }}</p>
          </div>
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">描述</h4>
            <p class="text-gray-700">{{ currentRole.description || '无描述' }}</p>
          </div>
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">关联策略</h4>
            <div v-if="currentRole.policies && currentRole.policies.length > 0">
              <span v-for="policy in currentRole.policies" :key="policy.id" class="inline-block px-2 py-1 bg-green-100 text-green-800 rounded mr-2 mb-2">
                {{ policy.name }}
              </span>
            </div>
            <span v-else class="text-gray-400">未关联任何策略</span>
          </div>
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">关联用户</h4>
            <div v-if="currentRole.users && currentRole.users.length > 0">
              <span v-for="user in currentRole.users" :key="user.id" class="inline-block px-2 py-1 bg-blue-100 text-blue-800 rounded mr-2 mb-2">
                {{ user.username }}
              </span>
            </div>
            <span v-else class="text-gray-400">未关联任何用户</span>
          </div>
          <div class="mb-2">
            <h4 class="text-sm font-medium text-gray-500">创建时间</h4>
            <p class="text-gray-700">{{ formatDate(currentRole.createdAt) }}</p>
          </div>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDetails" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            关闭
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">确认删除</h3>
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">确定要删除这个角色吗？此操作不可恢复。</p>
          <p v-if="(usersInCurrentRole > 0 || policiesInCurrentRole > 0)" class="text-sm text-red-500 mt-2">
            注意：该角色已关联 {{ usersInCurrentRole }} 个用户和 {{ policiesInCurrentRole }} 个策略，删除后这些关联将被移除。
          </p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            取消
          </button>
          <button @click="confirmDeleteRole" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            确认删除
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

// 模拟数据
const rolesList = ref([]);
const usersList = ref([]);
const policiesList = ref([]);
let nextRoleId = 1;

// 搜索关键字
const searchKeyword = ref('');

// 对话框状态
const dialogVisible = ref(false);
const detailsVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditMode = ref(false);
const currentRoleId = ref(null);
const currentRole = ref({});
const usersInCurrentRole = ref(0);
const policiesInCurrentRole = ref(0);

// 表单数据
const formData = ref({
  name: '',
  description: '',
  policyIds: [],
  userIds: []
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

// 加载模拟数据
const loadMockData = () => {
  // 模拟用户数据
  usersList.value = [
    { id: 1, username: 'admin', role: 'admin', createdAt: new Date('2023-01-15T10:00:00') },
    { id: 2, username: 'developer', role: 'user', createdAt: new Date('2023-01-20T14:30:00') },
    { id: 3, username: 'tester', role: 'user', createdAt: new Date('2023-01-25T09:15:00') }
  ];

  // 模拟策略数据
  policiesList.value = [
    {
      id: 1,
      name: '管理员策略',
      description: '拥有所有权限的管理员策略',
      document: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Action: ["admin:*"],
          },
          {
            Effect: "Allow",
            Action: ["kms:*"],
          },
          {
            Effect: "Allow",
            Action: ["s3:*"],
            Resource: ["arn:aws:s3:::*"]
          }
        ]
      },
      users: [],
      groups: [],
      createdAt: new Date('2023-01-15T10:00:00')
    },
    {
      id: 2,
      name: '只读策略',
      description: '只能读取数据的策略',
      document: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Action: ["s3:Get*", "s3:List*"],
            Resource: ["arn:aws:s3:::*"]
          }
        ]
      },
      users: [],
      groups: [],
      createdAt: new Date('2023-01-16T14:30:00')
    }
  ];

  // 模拟角色数据
  rolesList.value = [
    {
      id: 1,
      name: '系统管理员',
      description: '拥有系统管理权限的角色',
      policies: [policiesList.value[0]],
      users: [usersList.value[0]],
      createdAt: new Date('2023-01-15T10:00:00')
    },
    {
      id: 2,
      name: '只读用户',
      description: '只能读取数据的用户角色',
      policies: [policiesList.value[1]],
      users: [usersList.value[1], usersList.value[2]],
      createdAt: new Date('2023-01-16T14:30:00')
    }
  ];

  nextRoleId = 3;
};

// 显示添加角色对话框
const showAddRoleDialog = () => {
  isEditMode.value = false;
  formData.value = {
    name: '',
    description: '',
    policyIds: [],
    userIds: []
  };
  dialogVisible.value = true;
};

// 显示编辑角色对话框
const showEditRoleDialog = (role) => {
  isEditMode.value = true;
  currentRoleId.value = role.id;
  formData.value = {
    name: role.name,
    description: role.description,
    policyIds: role.policies.map(policy => policy.id),
    userIds: role.users.map(user => user.id)
  };
  dialogVisible.value = true;
};

// 显示角色详情
const showRoleDetails = (role) => {
  currentRole.value = role;
  detailsVisible.value = true;
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
  usersInCurrentRole.value = 0;
  policiesInCurrentRole.value = 0;
};

// 提交表单
const handleSubmit = () => {
  if (!formData.value.name) {
    showToastMessage('角色名称不能为空', 'error');
    return;
  }

  // 根据ID获取关联的策略和用户
  const selectedPolicies = policiesList.value.filter(policy => 
    formData.value.policyIds.includes(policy.id)
  );
  
  const selectedUsers = usersList.value.filter(user => 
    formData.value.userIds.includes(user.id)
  );

  if (isEditMode) {
    // 编辑角色
    const index = rolesList.value.findIndex(role => role.id === currentRoleId.value);
    if (index !== -1) {
      const currentRole = rolesList.value[index];
      const oldUsers = currentRole.users;
      
      // 更新角色信息
      rolesList.value[index] = {
        ...currentRole,
        name: formData.value.name,
        description: formData.value.description,
        policies: selectedPolicies,
        users: selectedUsers
      };
      
      // 找出被添加和被移除的用户
      const addedUsers = selectedUsers.filter(user => 
        !oldUsers.some(oldUser => oldUser.id === user.id)
      );
      const removedUsers = oldUsers.filter(oldUser => 
        !selectedUsers.some(user => user.id === oldUser.id)
      );
      
      // 更新用户中的角色信息
      addedUsers.forEach(user => {
        if (!user.roles) user.roles = [];
        if (!user.roles.some(role => role.id === currentRole.id)) {
          user.roles.push(currentRole);
        }
      });
      
      removedUsers.forEach(user => {
        if (user.roles) {
          user.roles = user.roles.filter(role => role.id !== currentRole.id);
        }
      });
      
      saveRoles();
      saveUsers(); // 保存用户数据
      showToastMessage('角色已更新', 'success');
    }
  } else {
    // 添加角色
    const newRole = {
      id: nextRoleId++,
      name: formData.value.name,
      description: formData.value.description,
      policies: selectedPolicies,
      users: selectedUsers,
      createdAt: new Date()
    };
    rolesList.value.push(newRole);
    
    // 更新用户中的角色信息
    selectedUsers.forEach(user => {
      if (!user.roles) user.roles = [];
      user.roles.push(newRole);
    });
    
    saveRoles();
    saveUsers(); // 保存用户数据
    showToastMessage('角色已创建', 'success');
  }

  closeDialog();
};

// 处理删除角色
const handleDeleteRole = (roleId) => {
  currentRoleId.value = roleId;
  const role = rolesList.value.find(r => r.id === roleId);
  if (role) {
    usersInCurrentRole.value = role.users ? role.users.length : 0;
    policiesInCurrentRole.value = role.policies ? role.policies.length : 0;
  }
  deleteDialogVisible.value = true;
};

// 确认删除角色
const confirmDeleteRole = () => {
  const index = rolesList.value.findIndex(role => role.id === currentRoleId.value);
  if (index !== -1) {
    const roleToDelete = rolesList.value[index];
    
    // 从关联的用户中移除该角色
    roleToDelete.users.forEach(user => {
      if (user.roles) {
        user.roles = user.roles.filter(role => role.id !== roleToDelete.id);
      }
    });
    
    rolesList.value.splice(index, 1);
    saveRoles();
    saveUsers(); // 保存用户数据
    showToastMessage('角色已删除', 'success');
  }
  closeDeleteDialog();
};

// 保存角色数据到本地存储
const saveRoles = () => {
  try {
    // 转换对象引用为ID引用以便存储
    const rolesToSave = rolesList.value.map(role => ({
      ...role,
      policies: role.policies.map(policy => policy.id),
      users: role.users.map(user => user.id),
      createdAt: role.createdAt.toISOString()
    }));
    localStorage.setItem('rolesList', JSON.stringify(rolesToSave));
  } catch (error) {
    console.error('保存角色列表失败:', error);
  }
};

// 保存用户数据到本地存储
const saveUsers = () => {
  try {
    // 转换对象引用为ID引用以便存储
    const usersToSave = usersList.value.map(user => ({
      ...user,
      groups: user.groups ? user.groups.map(group => group.id) : [],
      roles: user.roles ? user.roles.map(role => role.id) : [],
      createdAt: user.createdAt.toISOString()
    }));
    localStorage.setItem('users', JSON.stringify(usersToSave));
  } catch (error) {
    console.error('保存用户数据失败:', error);
  }
};

// 从本地存储加载角色数据
const loadRoles = () => {
  try {
    const savedRoles = localStorage.getItem('rolesList');
    if (savedRoles) {
      const parsedRoles = JSON.parse(savedRoles);
      rolesList.value = parsedRoles.map(role => ({
        ...role,
        createdAt: new Date(role.createdAt),
        // 恢复策略引用
        policies: policiesList.value.filter(policy => 
          role.policies.some(p => p === policy.id)
        ),
        // 恢复用户引用
        users: usersList.value.filter(user => 
          role.users.some(u => u === user.id)
        )
      }));
      // 更新下一个角色ID
      if (rolesList.value.length > 0) {
        nextRoleId = Math.max(...rolesList.value.map(role => role.id)) + 1;
      }
    }
  } catch (error) {
    console.error('加载角色列表失败:', error);
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
  loadRoles();
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
  
  th, td {
    padding: 0.75rem !important;
  }
  
  .max-w-3xl, .max-w-md {
    margin: 1rem;
    width: calc(100% - 2rem);
  }
  
  .flex-col {
    max-height: calc(100vh - 2rem);
  }
}
</style>