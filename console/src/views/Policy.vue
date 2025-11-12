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
        <input type="text" v-model="searchKeyword" :placeholder="t('policy.searchPlaceholder')"
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
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('policy.name') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('policy.description') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('policy.creationTime') }}</th>
              <th class="px-6 py-3 text-right text-sm font-medium text-gray-500 uppercase tracking-wider">{{
                t('policy.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="policy in filteredPolicies" :key="policy.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="flex items-center">
                  <div
                    class="w-10 h-10 rounded-full bg-green-100 flex items-center justify-center text-green-600 font-medium">
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
                  :disabled="['FullIamPolicy', 'FullS3Policy', 'FullConsolePolicy'].includes(policy.name)"
                  class="text-blue-600 hover:text-blue-900 transition-colors mr-3"
                  :class="{ 'opacity-50 cursor-not-allowed': ['FullIamPolicy', 'FullS3Policy', 'FullConsolePolicy'].includes(policy.name) }">
                  <i class="fas fa-edit mr-1"></i>{{ t('policy.edit') }}
                </button>
                <button @click="handleDeletePolicy(policy.id)"
                  :disabled="['FullIamPolicy', 'FullS3Policy', 'FullConsolePolicy'].includes(policy.name)"
                  class="text-red-600 hover:text-red-900 transition-colors"
                  :class="{ 'opacity-50 cursor-not-allowed': ['FullIamPolicy', 'FullS3Policy', 'FullConsolePolicy'].includes(policy.name) }">
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
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ isEditMode ? t('policy.editPolicy') : t('policy.addNewPolicy')
          }}</h3>
          <button @click="closeDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <form @submit.prevent="handleSubmit">
            <div class="mb-4">
              <label for="policyName" class="block text-sm font-medium text-gray-700 mb-1">{{ t('policy.name')
              }}</label>
              <input type="text" id="policyName" v-model="formData.name"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :placeholder="t('policy.pleaseEnterPolicyName')" required>
            </div>
            <div class="mb-4">
              <label for="description" class="block text-sm font-medium text-gray-700 mb-1">{{ t('policy.description')
              }}</label>
              <textarea id="description" v-model="formData.description"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                :placeholder="t('policy.pleaseEnterPolicyDescription')" rows="2"></textarea>
            </div>

            <!-- 策略文档 -->
            <div class="mb-6">
              <div class="flex items-center justify-between mb-2">
                <label for="policyDocument" class="block text-sm font-medium text-gray-700">{{
                  t('policy.policyDocument') }} (JSON)</label>
                <div class="flex gap-2">
                  <button type="button" @click="validateJson()"
                    class="text-sm text-blue-600 hover:text-blue-900 transition-colors">
                    {{ t('policy.validateJSON') }}
                  </button>
                  <button type="button" @click="formatJson()"
                    class="text-sm text-blue-600 hover:text-blue-900 transition-colors">
                    {{ t('policy.formatJSON') }}
                  </button>
                </div>
              </div>
              <div class="relative">
                <textarea id="policyDocument" v-model="formData.documentText"
                  class="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all font-mono text-sm"
                  rows="12"></textarea>
                <div v-if="jsonError" class="absolute top-1 right-1 text-sm text-red-500 bg-white px-2 py-0.5 rounded">
                  <i class="fas fa-exclamation-circle mr-1"></i>
                  {{ t('policy.jsonFormatError') }}
                </div>
                <div v-else-if="formData.documentText"
                  class="absolute top-1 right-1 text-sm text-green-500 bg-white px-2 py-0.5 rounded">
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
          <button @click="closeDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('common.cancel') }}
          </button>
          <button @click="handleSubmit"
            class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ isEditMode ? t('policy.update') : t('policy.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 策略详情对话框 -->
    <div v-if="detailsVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
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
            <pre
              class="bg-gray-50 p-4 rounded-lg text-sm font-mono overflow-x-auto whitespace-pre-wrap">{{ JSON.stringify(currentPolicy.document, null, 2) }}</pre>
          </div>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDetails"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
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
          <button @click="closeDeleteDialog" class="text-gray-500 hover:text-gray-700 transition-colors"
            aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('policy.confirmDeleteMessage') }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeDeleteDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('policy.cancel') }}
          </button>
          <button @click="confirmDeletePolicy"
            class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('policy.confirmDelete') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElMessage } from 'element-plus';
import { listpolicy, createpolicy, setpolicy, delpolicy } from '../api/admin.js';

// 获取翻译函数
const { t } = useI18n();

// ==================== 响应式数据定义 ====================
const policiesList = ref([]);
const searchKeyword = ref('');
const dialogVisible = ref(false);
const detailsVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditMode = ref(false);
const currentPolicyId = ref(null);
const currentPolicy = ref({});
const jsonError = ref(false);

const formData = ref({
  name: '',
  description: '',
  documentText: '',
  document: null
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

// ==================== 计算属性 ====================
const filteredPolicies = computed(() => {
  let filtered = policiesList.value;
  
  // 应用搜索过滤
  if (searchKeyword.value) {
    filtered = filtered.filter(policy =>
      policy.name.toLowerCase().includes(searchKeyword.value.toLowerCase()) ||
      (policy.description && policy.description.toLowerCase().includes(searchKeyword.value.toLowerCase()))
    );
  }
  
  // 按创建时间倒序排列（最新的在前）
  return filtered.sort((a, b) => b.createdAt - a.createdAt);
});

// ==================== 格式化函数 ====================
const formatDate = (date) => {
  if (!(date instanceof Date)) {
    date = new Date(date);
  }
  return date.toLocaleString('zh-CN');
};

// ==================== 对话框控制函数 ====================
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

const showPolicyDetails = (policy) => {
  currentPolicy.value = policy;
  detailsVisible.value = true;
};

const closeDialog = () => {
  dialogVisible.value = false;
  currentPolicyId.value = null;
  jsonError.value = false;
};

const closeDetails = () => {
  detailsVisible.value = false;
  currentPolicy.value = {};
};

const closeDeleteDialog = () => {
  deleteDialogVisible.value = false;
  currentPolicyId.value = null;
};

// ==================== JSON处理函数 ====================
const validateJson = () => {
  try {
    const document = JSON.parse(formData.value.documentText);
    jsonError.value = false;
    ElMessage.success(t('policy.formatCorrect'));
    return document;
  } catch (error) {
    jsonError.value = true;
    ElMessage.error(t('policy.jsonFormatError') + ': ' + error.message);
    return null;
  }
};

const formatJson = () => {
  try {
    const document = JSON.parse(formData.value.documentText);
    formData.value.documentText = JSON.stringify(document, null, 2);
    jsonError.value = false;
    ElMessage.success(t('policy.formatSuccess'));
  } catch (error) {
    ElMessage.error(t('policy.jsonFormatError') + ': ' + error.message);
  }
};

// ==================== 表单提交函数 ====================
const handleSubmit = async () => {
  if (!formData.value.name) {
    ElMessage.error(t('policy.nameRequired'));
    return;
  }

  // 验证JSON格式
  const document = validateJson();
  if (!document) {
    return;
  }

  // 验证策略版本
  if (document.Version !== '2012-10-17') {
    ElMessage.error(t('policy.policyVersionError'));
    return;
  }

  // 验证Statement是否存在
  if (!Array.isArray(document.Statement) || document.Statement.length === 0) {
    ElMessage.error(t('policy.missingStatement'));
    return;
  }

  const requestData = {
    name: formData.value.name,
    desc: formData.value.description || '',
    doc: JSON.stringify(document)
  };

  try {
    let response;
    if (isEditMode.value) {
      // 更新策略
      response = await setpolicy(requestData);
    } else {
      // 创建策略
      response = await createpolicy(requestData);
    }

    if (response.success || response.code === 0) {
      ElMessage.success(isEditMode.value ? t('policy.policyUpdated') : t('policy.policyCreated'));
      closeDialog();
      // 重新加载策略列表
      await loadPolicies();
    } else {
      ElMessage.error(response.msg || (isEditMode.value ? t('policy.updatePolicyFailed') : t('policy.createPolicyFailed')));
    }
  } catch (error) {
    let errorMessage = isEditMode.value ? t('policy.updatePolicyFailed') : t('policy.createPolicyFailed');
    if (error.response) {
      // 处理不同的错误状态码
      switch (error.response.status) {
        case 409:
          errorMessage = t('policy.policyExists');
          break;
        case 403:
          errorMessage = t('common.permissionDenied');
          break;
        case 400:
          errorMessage = t('policy.invalidPolicyDocument');
          break;
        default:
          errorMessage = error.response.data?.msg || error.response.data?.message || errorMessage;
      }
    }
    ElMessage.error(errorMessage);
  }
};

// ==================== 删除策略函数 ====================
const handleDeletePolicy = (policyId) => {
  currentPolicyId.value = policyId;
  deleteDialogVisible.value = true;
};

const confirmDeletePolicy = async () => {
  try {
    // 从策略列表中找到对应的策略名称
    const policy = policiesList.value.find(p => p.id === currentPolicyId.value);
    if (!policy) {
      ElMessage.error(t('policy.policyNotFound'));
      closeDeleteDialog();
      return;
    }

    const response = await delpolicy({ name: policy.name });

    if (response.success || response.code === 0) {
      ElMessage.success(t('policy.policyDeleted'));
      closeDeleteDialog();
      // 重新加载策略列表
      await loadPolicies();
    } else {
      ElMessage.error(response.msg || t('policy.deletePolicyFailed'));
    }
  } catch (error) {
    let errorMessage = t('policy.deletePolicyFailed');
    if (error.response) {
      // 处理不同的错误状态码
      switch (error.response.status) {
        case 404:
          errorMessage = t('policy.policyNotFound');
          break;
        case 403:
          errorMessage = t('common.permissionDenied');
          break;
        default:
          errorMessage = error.response.data?.msg || error.response.data?.message || errorMessage;
      }
    }
    ElMessage.error(errorMessage);
  }
};

// ==================== 数据加载函数 ====================
const loadPolicies = async () => {
  try {
    const response = await listpolicy();

    if (response.data) {
      // 清空旧数据
      policiesList.value = [];

      // 转换API返回的数据格式为前端需要的格式
      response.data.forEach(apiPolicy => {
        // 解析Document字符串为对象
        let documentObj = null;
        try {
          documentObj = JSON.parse(apiPolicy.document);
        } catch (error) {
          console.error('解析策略文档失败:', error);
          documentObj = defaultPolicyDocument;
        }

        policiesList.value.push({
          id: apiPolicy.name, // 使用名称作为ID
          name: apiPolicy.name,
          description: apiPolicy.description || '',
          document: documentObj,
          arn: apiPolicy.arn,
          createdAt: new Date(apiPolicy.createAt) // 使用服务器返回的创建时间
        });
      });
    }
  } catch (error) {
    ElMessage.error(t('policy.loadPolicyFailed'));
    console.error('加载策略列表失败:', error);
  }
};

// ==================== 生命周期 ====================
onMounted(async () => {
  await loadPolicies();
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