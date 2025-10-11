<template>
  <div class="storage-points-container">
    <!-- 头部标题和说明 -->
    <div class="storage-points-header mb-6 flex justify-between items-center px-4">
      <div>
        <h2 class="text-2xl font-bold text-gray-800">{{ t('endpoint.pageTitle') }}</h2>
        <p class="text-gray-500 mt-2">{{ t('endpoint.pageDescription') }}</p>
      </div>

      <!-- 按钮区域 -->
      <div class="flex gap-3 ml-8">
        <button @click="handleAddNew"
          class="inline-flex w-fit px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 items-center gap-2">
          <i class="fas fa-plus"></i>
          <span class="whitespace-nowrap">{{ t('endpoint.addStoragePoint') }}</span>
        </button>
        <button v-if="isFormVisible" @click="cancelEditing"
          class="inline-flex w-fit px-4 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg shadow transition-all duration-300 items-center gap-2">
          <i class="fas fa-times"></i>
          <span class="whitespace-nowrap">{{ t('endpoint.cancel') }}</span>
        </button>
      </div>
    </div>

    <!-- 存储点列表卡片 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 无存储点提示 -->
      <div class="mb-4">
        <p v-if="storagePointsList.length === 0" class="text-gray-500">{{ t('endpoint.noStoragePoints') }}</p>
      </div>

      <!-- 存储点列表 -->
      <div v-if="storagePointsList.length > 0" class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('endpoint.id') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('endpoint.class') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('endpoint.type') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider max-w-[300px]">{{ t('endpoint.configuration') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('endpoint.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="point in storagePointsList" :key="point.id">
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{{ point.id }}</td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                <span
                  class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">
                  {{ getStorageTypeName(point.type) }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{{ point.storage === 'disk' ? t('endpoint.diskStorage') :
                t('endpoint.s3CompatibleStorage') }}</td>
              <td class="px-6 py-4 text-sm text-gray-500 max-w-[300px] overflow-hidden text-ellipsis whitespace-nowrap">
                {{ point.storage === 'disk' ? point.path : point.bucket }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                <button @click="handleDelete(point.id)" class="text-red-600 hover:text-red-900">
                  {{ t('endpoint.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 配置表单卡片 -->
    <div v-if="isFormVisible"
      class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 表单内容 -->
      <form @submit.prevent="handleSave" class="space-y-6">
        <!-- 存储点类型选择 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">{{ t('endpoint.storagePointClass') }}</h3>
          <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div @click="storagePointClass = 'standard'" :class="[
              'border rounded-lg p-4 cursor-pointer transition-all duration-300',
              storagePointClass === 'standard'
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
            ]">
              <div class="flex items-center">
                <div class="w-5 h-5 rounded-full border-2 flex items-center justify-center mr-3">
                  <div v-if="storagePointClass === 'standard'" class="w-3 h-3 rounded-full bg-blue-500"></div>
                </div>
                <div>
                  <div class="font-medium text-gray-800">{{ t('endpoint.storageType.standard.label') }}</div>
                  <div class="text-xs text-gray-500 mt-1">{{ t('endpoint.storageType.standard.description') }}</div>
                </div>
              </div>
            </div>
            
            <div @click="storagePointClass = 'lowfreq'" :class="[
              'border rounded-lg p-4 cursor-pointer transition-all duration-300',
              storagePointClass === 'lowfreq'
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
            ]">
              <div class="flex items-center">
                <div class="w-5 h-5 rounded-full border-2 flex items-center justify-center mr-3">
                  <div v-if="storagePointClass === 'lowfreq'" class="w-3 h-3 rounded-full bg-blue-500"></div>
                </div>
                <div>
                  <div class="font-medium text-gray-800">{{ t('endpoint.storageType.lowfreq.label') }}</div>
                  <div class="text-xs text-gray-500 mt-1">{{ t('endpoint.storageType.lowfreq.description') }}</div>
                </div>
              </div>
            </div>
            
            <div @click="storagePointClass = 'archive'" :class="[
              'border rounded-lg p-4 cursor-pointer transition-all duration-300',
              storagePointClass === 'archive'
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
            ]">
              <div class="flex items-center">
                <div class="w-5 h-5 rounded-full border-2 flex items-center justify-center mr-3">
                  <div v-if="storagePointClass === 'archive'" class="w-3 h-3 rounded-full bg-blue-500"></div>
                </div>
                <div>
                  <div class="font-medium text-gray-800">{{ t('endpoint.storageType.archive.label') }}</div>
                  <div class="text-xs text-gray-500 mt-1">{{ t('endpoint.storageType.archive.description') }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 存储点类别选择 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">{{ t('endpoint.storagePointType') }}</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div @click="storageType = 'disk'" :class="[
              'border rounded-lg p-4 cursor-pointer transition-all duration-300',
              storageType === 'disk'
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
            ]">
              <div class="flex items-center">
                <div class="w-5 h-5 rounded-full border-2 flex items-center justify-center mr-3">
                  <div v-if="storageType === 'disk'" class="w-3 h-3 rounded-full bg-blue-500"></div>
                </div>
                <div>
                  <div class="font-medium text-gray-800">{{ t('endpoint.diskStorage') }}</div>
                  <div class="text-xs text-gray-500 mt-1">{{ t('endpoint.diskStorageDescription') }}</div>
                </div>
              </div>
            </div>
            <div @click="storageType = 's3'" :class="[
              'border rounded-lg p-4 cursor-pointer transition-all duration-300',
              storageType === 's3'
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
            ]">
              <div class="flex items-center">
                <div class="w-5 h-5 rounded-full border-2 flex items-center justify-center mr-3">
                  <div v-if="storageType === 's3'" class="w-3 h-3 rounded-full bg-blue-500"></div>
                </div>
                <div>
                  <div class="font-medium text-gray-800">{{ t('endpoint.s3CompatibleStorage') }}</div>
                  <div class="text-xs text-gray-500 mt-1">{{ t('endpoint.s3StorageDescription') }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 磁盘配置字段 -->
        <div v-if="storageType === 'disk'" class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">{{ t('endpoint.diskConfiguration') }}</h3>
          <div class="space-y-4">
            <div class="form-field">
              <label for="diskPath" class="block text-sm font-medium text-gray-700 mb-1">{{ t('endpoint.absolutePath') }}</label>
              <input id="diskPath" v-model="diskPath" type="text" :placeholder="t('endpoint.pathExample')"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
              <p class="text-xs text-gray-500 mt-1">{{ t('endpoint.pathPermissionNotice') }}</p>
            </div>
          </div>
        </div>

        <!-- S3配置字段 -->
        <div v-if="storageType === 's3'" class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">{{ t('endpoint.s3Configuration') }}</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="form-field">
              <label for="accessKey" class="block text-sm font-medium text-gray-700 mb-1">{{ t('endpoint.accessKey') }}</label>
              <input id="accessKey" v-model="s3Config.accessKey" type="text" :placeholder="t('endpoint.enterS3AccessKey')"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="secretKey" class="block text-sm font-medium text-gray-700 mb-1">{{ t('endpoint.secretKey') }}</label>
              <input id="secretKey" v-model="s3Config.secretKey" type="password" :placeholder="t('endpoint.enterS3SecretKey')"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="region" class="block text-sm font-medium text-gray-700 mb-1">{{ t('endpoint.region') }}</label>
              <input id="region" v-model="s3Config.region" type="text" :placeholder="t('endpoint.regionExample')"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="endpoint" class="block text-sm font-medium text-gray-700 mb-1">{{ t('endpoint.endpointLabel') }}</label>
              <input id="endpoint" v-model="s3Config.endpoint" type="text" :placeholder="t('endpoint.enterS3Endpoint')"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="bucket" class="block text-sm font-medium text-gray-700 mb-1">{{ t('endpoint.bucket') }}</label>
              <input id="bucket" v-model="s3Config.bucket" type="text" :placeholder="t('endpoint.enterBucketName')"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field flex flex-col justify-center">
              <label for="usePathStyle" class="flex items-center text-sm font-medium text-gray-700 my-0">
                <input id="usePathStyle" v-model="s3Config.usePathStyle" type="checkbox"
                  class="w-4 h-4 my-0 text-blue-600 border-gray-300 rounded focus:ring-blue-500 transition-all duration-300" />
                <span class="ml-2">{{ t('endpoint.usePathStyle') }}</span>
              </label>
            </div>
          </div>
        </div>

        <!-- 存储点ID -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">{{ t('endpoint.storagePointId') }}</h3>
          <div class="form-field">
            <input v-model="storagePointId" type="text" :placeholder="t('endpoint.uniqueIdentifier')"
              class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            <p class="text-xs text-gray-500 mt-1">{{ t('endpoint.idDescription') }}</p>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="form-actions flex justify-end gap-4 pt-4 border-t border-gray-100">
          <button type="button" @click="handleTest"
            class="px-6 py-2 border border-blue-600 text-blue-600 rounded-lg hover:bg-blue-50 transition-all duration-300 flex items-center gap-2">
            <i class="fas fa-check-circle"></i>
            <span>{{ t('endpoint.testConfiguration') }}</span>
          </button>
          <button type="submit"
            class="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow-md hover:shadow-lg transition-all duration-300 flex items-center gap-2">
            <i class="fas fa-save"></i>
            <span>{{ t('endpoint.saveConfiguration') }}</span>
          </button>
        </div>
      </form>
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

    <!-- 自定义确认对话框 -->
    <div v-if="showConfirmDialog" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50 transition-opacity duration-300">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden transition-all duration-300 transform animate-slideUp">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('endpoint.confirmDeleteTitle') }}</h3>
          <button @click="handleConfirmCancel" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ t('endpoint.confirmDeleteMessage') }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="handleConfirmCancel" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('endpoint.cancel') }}
          </button>
          <button @click="handleConfirmOk" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('endpoint.confirmDelete') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { liststorage, createstorage, delstorage, teststorage } from '@/api/admin.js';

// 国际化
const { t } = useI18n();

// 响应式变量定义
const storagePointClass = ref('standard');
const storageType = ref('disk');
const diskPath = ref('');
const s3Config = ref({
  accessKey: '',
  secretKey: '',
  region: '',
  endpoint: '',
  bucket: '',
  usePathStyle: false
});
const storagePointId = ref('');
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');
const showConfirmDialog = ref(false);
const confirmDialogCallback = ref(null);
const storagePointsList = ref([]);
const isFormVisible = ref(false);

// 工具函数
const generateStoragePointId = () => {
  const timestamp = Date.now();
  const random = Math.floor(Math.random() * 1000);
  const typeMap = {
    standard: 'std',
    lowfreq: 'lfr',
    archive: 'arc'
  };
  const storageMap = {
    disk: 'dsk',
    s3: 's3'
  };
  return `${typeMap[storagePointClass.value]}-${storageMap[storageType.value]}-${timestamp}-${random}`;
};

const updateStoragePointId = () => {
  if (!storagePointId.value || storagePointId.value === generateStoragePointId()) {
    storagePointId.value = generateStoragePointId();
  }
};

const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  // 3秒后自动隐藏
  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

const getStorageTypeName = (type) => {
  try {
    const translationKey = `endpoint.storageType.${type}.label`;
    const translated = t(translationKey);
    // 检查是否是原始键值（翻译失败时返回原始键）
    if (translated !== translationKey) {
      return translated;
    }
    return type;
  } catch (error) {
    console.error('Error getting storage type name:', error);
    return type;
  }
};

const mapStorageClassToDisplayName = (storageClass) => {
  const classMap = {
    'STANDARD_IA': t('endpoint.storageType.lowfreq.label'),
    'GLACIER_IR': t('endpoint.storageType.archive.label')
  };
  return classMap[storageClass] || storageClass.toLowerCase();
};

// 数据加载函数
const loadStoragePoints = async () => {
  try {
    const result = await liststorage();
    if (result.code === 0 && result.data) {
      // 转换API返回的数据格式以匹配前端显示需求
      storagePointsList.value = result.data.map(item => ({
        id: item.storageID,
        // 映射storageClass为中文显示名称
        type: mapStorageClassToDisplayName(item.storageClass),
        storage: item.storageType.toLowerCase(),
        // 修复disk和s3属性的大小写问题，确保配置列正确显示path或bucket
        ...(item.storageType.toLowerCase() === 'disk' && item.disk ? { path: item.disk.path } : {}),
        ...(item.storageType.toLowerCase() === 's3' && item.s3 ? item.s3 : {})
      }));
    } else {
      showToastMessage(result.msg || '加载存储点列表失败', 'error');
    }
  } catch (error) {
    console.error('加载存储点列表失败:', error);
    showToastMessage('加载存储点列表失败', 'error');
  }
};

// 表单操作函数
const resetForm = () => {
  storagePointClass.value = 'standard';
  storageType.value = 'disk';
  diskPath.value = '';
  s3Config.value = {
    accessKey: '',
    secretKey: '',
    region: '',
    endpoint: '',
    bucket: '',
    usePathStyle: false
  };
  storagePointId.value = generateStoragePointId();
};

const handleAddNew = () => {
  resetForm();
  isFormVisible.value = true;
};

const cancelEditing = () => {
  resetForm();
  isFormVisible.value = false;
};

// 配置测试函数
const handleTest = async () => {
  try {
    // 验证必填字段
    if (storageType.value === 'disk') {
      if (!diskPath.value) {
        throw new Error(t('endpoint.enterDiskPath'));
      }
      if (!diskPath.value.startsWith('/')) {
        throw new Error(t('endpoint.enterValidLinuxPath'));
      }
    } else if (storageType.value === 's3') {
      if (!s3Config.value.accessKey || !s3Config.value.secretKey) {
        throw new Error(t('endpoint.enterAccessKeys'));
      }
      if (!s3Config.value.bucket) {
        throw new Error(t('endpoint.enterBucketName'));
      }
      if (!s3Config.value.region && !s3Config.value.endpoint) {
        throw new Error(t('endpoint.enterRegionOrEndpoint'));
      }
    }

    // 构建请求参数
    const requestData = {
      StorageID: storagePointId.value,
      StorageClass: storagePointClass.value,
      StorageType: storageType.value.toUpperCase(),
      ...(storageType.value === 'disk' ? { Disk: { path: diskPath.value } } : { S3: s3Config.value })
    };

    // 调用测试API
    const result = await teststorage(requestData);
    
    if (result.code === 0) {
      showToastMessage(t('endpoint.testSuccess'), 'success');
    } else {
      showToastMessage(result.msg || '配置测试失败', 'error');
    }
  } catch (error) {
    showToastMessage(error.message, 'error');
  }
};

// 保存配置函数
const handleSave = async () => {
  try {
    // 验证必填字段
    if (storageType.value === 'disk') {
      if (!diskPath.value) {
        throw new Error(t('endpoint.enterDiskPath'));
      }
      if (!diskPath.value.startsWith('/')) {
        throw new Error(t('endpoint.enterValidLinuxPath'));
      }
    } else if (storageType.value === 's3') {
      if (!s3Config.value.accessKey || !s3Config.value.secretKey || !s3Config.value.bucket) {
        throw new Error(t('endpoint.enterCompleteS3Config'));
      }
    }

    if (!storagePointId.value) {
      throw new Error(t('endpoint.enterStoragePointId'));
    }

    // 构建API请求对象
    const requestData = {
      StorageID: storagePointId.value,
      StorageClass: storagePointClass.value,
      StorageType: storageType.value,
      ...(storageType.value === 'disk' ? { Disk: { path: diskPath.value } } : {}),
      ...(storageType.value === 's3' ? { S3: s3Config.value } : {})
    };

    // 调用API创建存储点
    const result = await createstorage(requestData);
    if (result.code === 0) {
      // 重新加载列表以确保数据一致性
      await loadStoragePoints();
      showToastMessage(t('endpoint.saveSuccess'), 'success');
      cancelEditing();
    } else {
      showToastMessage(result.msg || '保存失败', 'error');
    }
  } catch (error) {
    console.error('保存配置失败:', error);
    showToastMessage(error.message, 'error');
  }
};

// 删除存储点函数
const handleDelete = (id) => {
  confirmDialogCallback.value = async () => {
    try {
      const result = await delstorage({ storageID: id });
      if (result.code === 0) {
        // 重新加载列表以确保数据一致性
        await loadStoragePoints();
        showToastMessage(t('endpoint.deleteSuccess'), 'success');
      } else {
        showToastMessage(result.msg || '删除失败', 'error');
      }
    } catch (error) {
      console.error('删除存储点失败:', error);
      showToastMessage('删除存储点失败', 'error');
    }
  };
  showConfirmDialog.value = true;
};

// 确认对话框函数
const handleConfirmCancel = () => {
  showConfirmDialog.value = false;
  confirmDialogCallback.value = null;
};

const handleConfirmOk = () => {
  if (typeof confirmDialogCallback.value === 'function') {
    confirmDialogCallback.value();
  }
  showConfirmDialog.value = false;
  confirmDialogCallback.value = null;
};

// 生命周期钩子
onMounted(() => {
  loadStoragePoints();
});

// 监听类型变化，自动更新存储点ID
watch([storagePointClass, storageType], () => {
  updateStoragePointId();
});
</script>

<style scoped>
.storage-points-container {
  background-color: #f5f7fa;
  min-height: calc(100vh - 120px);
}

.storage-points-header {
  padding-top: 1rem;
}

.config-card {
  background: white;
  border-radius: 12px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
  transition: all 0.3s ease;
}

.config-card:hover {
  box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
}

.form-section {
  margin-bottom: 1.5rem;
}

.form-section h3 {
  font-size: 1.1rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 1rem;
}

.form-field {
  margin-bottom: 1rem;
}

.form-field label {
  display: block;
  font-size: 0.875rem;
  font-weight: 500;
  color: #374151;
  margin-bottom: 0.25rem;
}

.form-field input[type="text"],
.form-field input[type="password"] {
  width: 100%;
  padding: 0.5rem 0.75rem;
  border: 1px solid #d1d5db;
  border-radius: 0.5rem;
  font-size: 0.875rem;
  transition: all 0.3s ease;
}

.form-field input[type="text"]:focus,
.form-field input[type="password"]:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.form-field input[type="checkbox"] {
  width: 1rem;
  height: 1rem;
  border: 1px solid #d1d5db;
  border-radius: 0.25rem;
  transition: all 0.3s ease;
}

.form-field input[type="checkbox"]:checked {
  background-color: #3b82f6;
  border-color: #3b82f6;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 1rem;
  padding-top: 1rem;
  border-top: 1px solid #f3f4f6;
}

.form-actions button {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  border-radius: 0.5rem;
  font-size: 0.875rem;
  font-weight: 500;
  transition: all 0.3s ease;
}

.form-actions button:first-child {
  border: 1px solid #3b82f6;
  background-color: white;
  color: #3b82f6;
}

.form-actions button:first-child:hover {
  background-color: #eff6ff;
}

.form-actions button:last-child {
  background-color: #3b82f6;
  color: white;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
}

.form-actions button:last-child:hover {
  background-color: #2563eb;
  box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
}

/* 卡片样式 */
.grid>div {
  border: 1px solid #e5e7eb;
  border-radius: 0.5rem;
  padding: 1rem;
  transition: all 0.3s ease;
}

.grid>div:hover {
  border-color: #93c5fd;
  background-color: #f9fafb;
}

.grid>div.active {
  border-color: #3b82f6;
  background-color: #eff6ff;
}

/* 提示框样式 */
.fixed {
  position: fixed;
  z-index: 50;
}

.top-4 {
  top: 1rem;
}

.right-4 {
  right: 1rem;
}

.px-4 {
  padding-left: 1rem;
  padding-right: 1rem;
}

.py-3 {
  padding-top: 0.75rem;
  padding-bottom: 0.75rem;
}

.rounded-lg {
  border-radius: 0.5rem;
}

.shadow-lg {
  box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
}

.transition-all {
  transition-property: all;
  transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1);
  transition-duration: 300ms;
}

.duration-300 {
  transition-duration: 300ms;
}

.z-50 {
  z-index: 50;
}

.bg-green-500 {
  background-color: #10b981;
}

.bg-red-500 {
  background-color: #ef4444;
}

.text-white {
  color: white;
}

.flex {
  display: flex;
}

.items-center {
  align-items: center;
}

.gap-2 {
  gap: 0.5rem;
}

/* 响应式调整 */
@media (max-width: 768px) {
  .grid-cols-1,
  .grid-cols-2,
  .grid-cols-3 {
    grid-template-columns: repeat(1, minmax(0, 1fr));
  }
}

/* 确认对话框样式 */
.confirm-dialog-overlay {
  animation: fadeIn 0.2s ease-out;
}

.confirm-dialog {
  animation: slideUp 0.3s ease-out;
}

@keyframes fadeIn {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

@keyframes slideUp {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 确认对话框样式 */
.confirm-dialog {
  animation: slideUp 0.3s ease-out;
}

/* 确认对话框遮罩层动画 */
.dialog-overlay {
  animation: fadeIn 0.3s ease-out;
}

@media (max-width: 768px) {
  .storage-points-container {
    padding: 1rem;
  }
  .confirm-dialog {
    margin: 1rem;
    width: calc(100% - 2rem);
  }
}
</style>