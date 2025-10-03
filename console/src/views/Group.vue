<template>
  <div class="groups-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">用户组管理</h1>
      <button @click="showAddGroupDialog" 
              class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
        <i class="fas fa-plus"></i>
        <span>添加用户组</span>
      </button>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" 
               v-model="searchKeyword" 
               placeholder="搜索用户组名称..." 
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
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">名称</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">描述</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">用户数量</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">策略数量</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">创建时间</th>
              <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">操作</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="group in filteredGroups" :key="group.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div class="w-10 h-10 rounded-full bg-purple-100 flex items-center justify-center text-purple-600 font-medium">
                    {{ group.name.charAt(0).toUpperCase() }}
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ group.name }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 text-sm text-gray-500">
                {{ group.description || '无描述' }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-xs font-medium rounded-full bg-blue-100 text-blue-800">
                  {{ group.users ? group.users.length : 0 }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 text-xs font-medium rounded-full bg-green-100 text-green-800">
                  {{ group.policies ? group.policies.length : 0 }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(group.createdAt) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showEditGroupDialog(group)" 
                        class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-edit mr-1"></i>编辑
                </button>
                <button @click="handleDeleteGroup(group.id)" 
                        class="text-red-600 hover:text-red-900 transition-colors">
                  <i class="fas fa-trash-alt mr-1"></i>删除
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- 空状态 -->
      <div v-if="filteredGroups.length === 0" class="py-12 text-center">
        <div class="text-gray-400 mb-4">
          <i class="fas fa-users-slash text-4xl"></i>
        </div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">暂无用户组</h3>
        <p class="text-gray-500 mb-6">点击上方"添加用户组"按钮创建第一个用户组</p>
      </div>
    </div>

    <!-- 添加/编辑用户组对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? '编辑用户组' : '添加用户组' }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="groupName" class="block text-sm font-medium text-gray-700 mb-1">用户组名称</label>
              <input type="text" id="groupName" v-model="formData.name" 
                     class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                     required>
            </div>
            <div class="mb-4">
              <label for="description" class="block text-sm font-medium text-gray-700 mb-1">描述</label>
              <textarea id="description" v-model="formData.description" 
                        class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                        rows="3"></textarea>
            </div>
            
            <!-- 关联用户 -->
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-2">关联用户</label>
              <div class="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto">
                <div v-for="user in usersList" :key="user.id" class="flex items-center">
                  <input type="checkbox" 
                         :id="`user-${user.id}`" 
                         :checked="formData.users.includes(user.id)"
                         @change="handleUserToggle(user.id)" 
                         class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`user-${user.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ user.username }}
                    <span v-if="user.role === 'admin'" class="text-xs text-blue-500 ml-1">(管理员)</span>
                  </label>
                </div>
              </div>
            </div>
            
            <!-- 关联策略 -->
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-2">关联策略</label>
              <div class="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto">
                <div v-for="policy in policiesList" :key="policy.id" class="flex items-center">
                  <input type="checkbox" 
                         :id="`policy-${policy.id}`" 
                         :checked="formData.policies.includes(policy.id)"
                         @change="handlePolicyToggle(policy.id)" 
                         class="w-4 h-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded">
                  <label :for="`policy-${policy.id}`" class="ml-2 block text-sm text-gray-700">
                    {{ policy.name }}
                  </label>
                </div>
              </div>
              <div v-if="policiesList.length === 0" class="text-gray-400 text-sm mt-1">暂无可用策略</div>
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
          <p class="text-gray-700">确定要删除这个用户组吗？此操作不可恢复。</p>
          <p v-if="usersInCurrentGroup > 0" class="text-sm text-red-500 mt-2">
            注意：该用户组中还有 {{ usersInCurrentGroup }} 个用户，删除后这些用户将不再属于该组。
          </p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            取消
          </button>
          <button @click="confirmDeleteGroup" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
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
const groupsList = ref([]);
const usersList = ref([]);
const policiesList = ref([]);
let nextGroupId = 1;

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

// 过滤用户组列表
const filteredGroups = computed(() => {
  if (!searchKeyword.value) {
    return groupsList.value;
  }
  return groupsList.value.filter(group => 
    group.name.toLowerCase().includes(searchKeyword.value.toLowerCase()) ||
    (group.description && group.description.toLowerCase().includes(searchKeyword.value.toLowerCase()))
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
      createdAt: new Date('2023-01-16T14:30:00')
    }
  ];

  // 模拟用户组数据
  groupsList.value = [
    { 
      id: 1, 
      name: '开发组', 
      description: '开发人员用户组', 
      users: [usersList.value[1]],
      policies: [policiesList.value[1]],
      createdAt: new Date('2023-01-15T10:00:00')
    },
    { 
      id: 2, 
      name: '测试组', 
      description: '测试人员用户组', 
      users: [usersList.value[2]],
      policies: [],
      createdAt: new Date('2023-01-16T14:30:00')
    },
    { 
      id: 3, 
      name: '管理员组', 
      description: '系统管理员用户组', 
      users: [usersList.value[0]],
      policies: [policiesList.value[0]],
      createdAt: new Date('2023-01-17T09:15:00')
    }
  ];

  nextGroupId = 4;
};

// 显示添加用户组对话框
const showAddGroupDialog = () => {
  isEditMode.value = false;
  formData.value = {
    name: '',
    description: '',
    users: [],
    policies: []
  };
  dialogVisible.value = true;
};

// 显示编辑用户组对话框
const showEditGroupDialog = (group) => {
  isEditMode.value = true;
  currentGroupId.value = group.id;
  formData.value = {
    name: group.name,
    description: group.description,
    users: group.users.map(user => user.id),
    policies: group.policies.map(policy => policy.id)
  };
  dialogVisible.value = true;
};

// 关闭对话框
const closeDialog = () => {
  dialogVisible.value = false;
  currentGroupId.value = null;
};

// 关闭删除对话框
const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentGroupId.value = null;
  usersInCurrentGroup.value = 0;
};

// 处理用户选择
const handleUserToggle = (userId) => {
  const index = formData.value.users.indexOf(userId);
  if (index > -1) {
    formData.value.users.splice(index, 1);
  } else {
    formData.value.users.push(userId);
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
  if (!formData.value.name) {
    showToastMessage('用户组名称不能为空', 'error');
    return;
  }

  // 获取完整的用户和策略信息
  const selectedUsers = usersList.value.filter(user => 
    formData.value.users.includes(user.id)
  );
  
  const selectedPolicies = policiesList.value.filter(policy => 
    formData.value.policies.includes(policy.id)
  );

  if (isEditMode) {
    // 编辑用户组
    const index = groupsList.value.findIndex(group => group.id === currentGroupId.value);
    if (index !== -1) {
      groupsList.value[index] = {
        ...groupsList.value[index],
        name: formData.value.name,
        description: formData.value.description,
        users: selectedUsers,
        policies: selectedPolicies
      };
      saveGroups();
      showToastMessage('用户组已更新', 'success');
    }
  } else {
    // 添加用户组
    const newGroup = {
      id: nextGroupId++,
      name: formData.value.name,
      description: formData.value.description,
      users: selectedUsers,
      policies: selectedPolicies,
      createdAt: new Date()
    };
    groupsList.value.push(newGroup);
    saveGroups();
    showToastMessage('用户组已创建', 'success');
  }

  closeDialog();
};

// 处理删除用户组
const handleDeleteGroup = (groupId) => {
  currentGroupId.value = groupId;
  const group = groupsList.value.find(g => g.id === groupId);
  if (group && group.users) {
    usersInCurrentGroup.value = group.users.length;
  }
  deleteDialogVisible.value = true;
};

// 确认删除用户组
const confirmDeleteGroup = () => {
  const index = groupsList.value.findIndex(group => group.id === currentGroupId.value);
  if (index !== -1) {
    groupsList.value.splice(index, 1);
    saveGroups();
    showToastMessage('用户组已删除', 'success');
  }
  closeDeleteDialog();
};

// 保存用户组数据到本地存储
const saveGroups = () => {
  try {
    localStorage.setItem('groupsList', JSON.stringify(groupsList.value));
  } catch (error) {
    console.error('保存用户组列表失败:', error);
  }
};

// 从本地存储加载用户组数据
const loadGroups = () => {
  try {
    const savedGroups = localStorage.getItem('groupsList');
    if (savedGroups) {
      groupsList.value = JSON.parse(savedGroups);
      // 恢复日期对象
      groupsList.value.forEach(group => {
        group.createdAt = new Date(group.createdAt);
        // 恢复用户引用
        group.users = usersList.value.filter(user => 
          group.users.some(u => u.id === user.id || u === user.id)
        );
        // 恢复策略引用
        group.policies = policiesList.value.filter(policy => 
          group.policies.some(p => p.id === policy.id || p === policy.id)
        );
      });
      // 更新下一个用户组ID
      if (groupsList.value.length > 0) {
        nextGroupId = Math.max(...groupsList.value.map(group => group.id)) + 1;
      }
    }
  } catch (error) {
    console.error('加载用户组列表失败:', error);
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
  loadGroups();
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
  
  th, td {
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