<template>
  <div class="policies-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">{{ t('policy.pageTitle') }}</h1>
      <button @click="showAddPolicyDialog" 
              class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors shadow-sm">
        <i class="fas fa-plus"></i>
        <span>{{ t('policy.addPolicy') }}</span>
      </button>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6">
      <div class="relative">
        <input type="text" 
               v-model="searchKeyword" 
               :placeholder="t('policy.searchPlaceholder')" 
               class="w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 策略列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class=" text-sm bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('policy.name') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('policy.description') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('policy.creationTime') }}</th>
              <th class="px-6 py-3 text-right text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('policy.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="policy in filteredPolicies" :key="policy.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div class="w-10 h-10 rounded-full bg-green-100 flex items-center justify-center text-green-600 font-medium">
                    <i class="fas fa-file-signature"></i>
                  </div>
                  <div class="ml-4">
                    <div class="text-sm font-medium text-gray-900">{{ policy.name }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 text-sm text-gray-500">
                {{ policy.description || t('policy.noDescription') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(policy.createdAt) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showPolicyDetails(policy)" 
                        class="text-green-600 hover:text-green-900 transition-colors mr-3">
                  <i class="fas fa-eye mr-1"></i>{{ t('policy.view') }}
                </button>
                <button @click="showEditPolicyDialog(policy)" 
                        class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-edit mr-1"></i>{{ t('policy.edit') }}
                </button>
                <button @click="handleDeletePolicy(policy.id)" 
                        class="text-red-600 hover:text-red-900 transition-colors">
                  <i class="fas fa-trash-alt mr-1"></i>{{ t('policy.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- 空状态 -->
      <div v-if="filteredPolicies.length === 0" class="py-12 text-center">
        <div class="text-gray-400 mb-4">
          <i class="fas fa-file-signature-slash text-4xl"></i>
        </div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('policy.noPolicies') }}</h3>
        <p class="text-gray-500 mb-6">{{ t('policy.clickAddPolicy') }}</p>
      </div>
    </div>

    <!-- 添加/编辑策略对话框 -->
    <div v-if="dialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? t('policy.editPolicy') : t('policy.addNewPolicy') }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="policyName" class="block text-sm font-medium text-gray-700 mb-1">{{ t('policy.name') }}</label>
              <input type="text" id="policyName" v-model="formData.name" 
                     class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                     :placeholder="t('policy.pleaseEnterPolicyName')" required>
            </div>
            <div class="mb-4">
              <label for="description" class="block text-sm font-medium text-gray-700 mb-1">{{ t('policy.description') }}</label>
              <textarea id="description" v-model="formData.description" 
                        class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" 
                        :placeholder="t('policy.pleaseEnterPolicyDescription')" rows="2"></textarea>
            </div>
            
            <!-- 策略文档 -->
            <div class="mb-6">
              <div class="flex items-center justify-between mb-2">
                <label for="policyDocument" class="block text-sm font-medium text-gray-700">{{ t('policy.policyDocument') }} (JSON)</label>
                <button type="button" @click="validateJson()" 
                        class="text-sm text-blue-600 hover:text-blue-900 transition-colors">
                  {{ t('policy.validateJSON') }}
                </button>
              </div>
              <div class="relative">
                <textarea id="policyDocument" v-model="formData.documentText" 
                          class="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all font-mono text-sm"
                          rows="12"></textarea>
                <div v-if="jsonError" class="absolute top-1 right-1 text-sm text-red-500 bg-white px-2 py-0.5 rounded">
                  <i class="fas fa-exclamation-circle mr-1"></i>
                  {{ t('policy.jsonFormatError') }}
                </div>
                <div v-else-if="formData.documentText" class="absolute top-1 right-1 text-sm text-green-500 bg-white px-2 py-0.5 rounded">
                  <i class="fas fa-check-circle mr-1"></i>
                  {{ t('policy.formatCorrect') }}
                </div>
              </div>
              <div class="mt-2 text-xs text-gray-500">
                {{ t('policy.useStandardS3Format') }}
              </div>
            </div>
          </form>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('policy.cancel') }}
          </button>
          <button @click="handleSubmit" class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ isEditMode ? t('policy.update') : t('policy.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 策略详情对话框 -->
    <div v-if="detailsVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('policy.policyDetails') }}</h3>
          <button @click="closeDetails" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">{{ t('policy.name') }}</h4>
            <p class="font-medium text-gray-900">{{ currentPolicy.name }}</p>
          </div>
          <div class="mb-4">
            <h4 class="text-sm font-medium text-gray-500">{{ t('policy.description') }}</h4>
            <p class="text-gray-700">{{ currentPolicy.description || t('policy.noDescription') }}</p>
          </div>

          <div class="mb-2">
            <h4 class="text-sm font-medium text-gray-500">{{ t('policy.creationTime') }}</h4>
            <p class="text-gray-700">{{ formatDate(currentPolicy.createdAt) }}</p>
          </div>
          <div class="mt-6">
            <h4 class="text-sm font-medium text-gray-500 mb-2">{{ t('policy.policyDocument') }}</h4>
            <pre class="bg-gray-50 p-4 rounded-lg text-sm font-mono overflow-x-auto whitespace-pre-wrap break-all">
              {{ JSON.stringify(currentPolicy.document, null, 2) }}
            </pre>
          </div>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDetails" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('policy.close') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('policy.confirmDelete') }}</h3>
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('policy.confirmDeleteMessage') }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('policy.cancel') }}
          </button>
          <button @click="confirmDeletePolicy" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('policy.confirmDelete') }}
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

// 获取翻译函数
const { t } = useI18n();

// 模拟数据
const policiesList = ref([]);
const usersList = ref([]);
const groupsList = ref([]);
let nextPolicyId = 1;

// 搜索关键字
const searchKeyword = ref('');

// 对话框状态
const dialogVisible = ref(false);
const detailsVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditMode = ref(false);
const currentPolicyId = ref(null);
const currentPolicy = ref({});
const jsonError = ref(false);

// 表单数据
const formData = ref({
  name: '',
  description: '',
  documentText: '', // 用于文本编辑
  document: null    // 用于对象存储
});

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 过滤策略列表
const filteredPolicies = computed(() => {
  if (!searchKeyword.value) {
    return policiesList.value;
  }
  return policiesList.value.filter(policy => 
    policy.name.toLowerCase().includes(searchKeyword.value.toLowerCase()) ||
    (policy.description && policy.description.toLowerCase().includes(searchKeyword.value.toLowerCase()))
  );
});

// 默认的策略文档模板
const defaultPolicyDocument = {
  Version: "2012-10-17",
  Statement: [
    {
      Effect: "Allow",
      Action: [],
      Resource: []
    }
  ]
};

// 加载模拟数据
const loadMockData = () => {
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

  nextPolicyId = 3;
};

// 显示添加策略对话框
const showAddPolicyDialog = () => {
  isEditMode.value = false;
  formData.value = {
    name: '',
    description: '',
    documentText: JSON.stringify(defaultPolicyDocument, null, 2),
    document: null
  };
  jsonError.value = false;
  dialogVisible.value = true;
};

// 显示编辑策略对话框
const showEditPolicyDialog = (policy) => {
  isEditMode.value = true;
  currentPolicyId.value = policy.id;
  formData.value = {
    name: policy.name,
    description: policy.description,
    documentText: JSON.stringify(policy.document, null, 2),
    document: policy.document
  };
  jsonError.value = false;
  dialogVisible.value = true;
};

// 显示策略详情
const showPolicyDetails = (policy) => {
  currentPolicy.value = policy;
  detailsVisible.value = true;
};

// 关闭对话框
const closeDialog = () => {
  dialogVisible.value = false;
  currentPolicyId.value = null;
  jsonError.value = false;
};

// 关闭详情对话框
const closeDetails = () => {
  detailsVisible.value = false;
  currentPolicy.value = {};
};

// 关闭删除对话框
const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentPolicyId.value = null;
};

// 验证JSON格式
const validateJson = () => {
  try {
    const document = JSON.parse(formData.value.documentText);
    jsonError.value = false;
    showToastMessage('JSON格式正确', 'success');
    return document;
  } catch (error) {
    jsonError.value = true;
    showToastMessage('JSON格式错误: ' + error.message, 'error');
    return null;
  }
};

// 提交表单
const handleSubmit = () => {
  if (!formData.value.name) {
    showToastMessage('策略名称不能为空', 'error');
    return;
  }

  // 验证JSON格式
  const document = validateJson();
  if (!document) {
    return;
  }

  // 验证策略版本
  if (document.Version !== '2012-10-17') {
    showToastMessage('策略版本必须为 2012-10-17', 'error');
    return;
  }

  // 验证Statement是否存在
  if (!Array.isArray(document.Statement) || document.Statement.length === 0) {
    showToastMessage('策略必须包含Statement数组', 'error');
    return;
  }

  if (isEditMode) {
    // 编辑策略
    const index = policiesList.value.findIndex(policy => policy.id === currentPolicyId.value);
    if (index !== -1) {
      // 保留原有的用户和组关联
      const existingPolicy = policiesList.value[index];
      policiesList.value[index] = {
        ...existingPolicy,
        name: formData.value.name,
        description: formData.value.description,
        document: document
      };
      savePolicies();
      showToastMessage('策略已更新', 'success');
    }
  } else {
    // 添加策略
    const newPolicy = {
      id: nextPolicyId++,
      name: formData.value.name,
      description: formData.value.description,
      document: document,
      users: [],
      groups: [],
      createdAt: new Date()
    };
    policiesList.value.push(newPolicy);
    savePolicies();
    showToastMessage('策略已创建', 'success');
  }

  closeDialog();
};

// 处理删除策略
const handleDeletePolicy = (policyId) => {
  currentPolicyId.value = policyId;
  const policy = policiesList.value.find(p => p.id === policyId);
  deleteDialogVisible.value = true;
};

// 确认删除策略
const confirmDeletePolicy = () => {
  const index = policiesList.value.findIndex(policy => policy.id === currentPolicyId.value);
  if (index !== -1) {
    policiesList.value.splice(index, 1);
    savePolicies();
    showToastMessage('策略已删除', 'success');
  }
  closeDeleteDialog();
};

// 保存策略数据到本地存储
const savePolicies = () => {
  try {
    // 转换对象引用为ID引用以便存储
    const policiesToSave = policiesList.value.map(policy => ({
      ...policy
    }));
    localStorage.setItem('policiesList', JSON.stringify(policiesToSave));
  } catch (error) {
    console.error('保存策略列表失败:', error);
  }
};

// 从本地存储加载策略数据
const loadPolicies = () => {
  try {
    const savedPolicies = localStorage.getItem('policiesList');
    if (savedPolicies) {
      const parsedPolicies = JSON.parse(savedPolicies);
      policiesList.value = parsedPolicies.map(policy => ({
        ...policy,
        createdAt: new Date(policy.createdAt)
      }));
      // 更新下一个策略ID
      if (policiesList.value.length > 0) {
        nextPolicyId = Math.max(...policiesList.value.map(policy => policy.id)) + 1;
      }
    }
  } catch (error) {
    console.error('加载策略列表失败:', error);
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
  loadPolicies();
});
</script>

<style scoped>
.policies-container {
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
  .policies-container {
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