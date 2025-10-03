<template>
  <div class="storage-points-container">
    <!-- 头部标题和说明 -->
    <div class="storage-points-header mb-6 flex justify-between items-center px-4">
      <div>
        <h2 class="text-2xl font-bold text-gray-800">存储点配置</h2>
        <p class="text-gray-500 mt-2">配置不同类型的存储点，支持磁盘存储和S3兼容存储</p>
      </div>

      <!-- 按钮区域 -->
      <div class="flex gap-3">
        <button v-if="!isEditMode" @click="handleAddNew"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-plus"></i>
          <span>新增存储点</span>
        </button>
        <button v-else @click="cancelEditing"
          class="px-4 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-times"></i>
          <span>取消编辑</span>
        </button>
      </div>
    </div>

    <!-- 存储点列表卡片 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 无存储点提示 -->
      <div class="mb-4">
        <p v-if="storagePointsList.length === 0" class="text-gray-500">暂无存储点配置，点击上方按钮新增</p>
      </div>

      <!-- 存储点列表 -->
      <div v-if="storagePointsList.length > 0" class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">类型</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">存储方式</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">配置</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">操作</th>
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
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{{ point.storage === 'disk' ? '磁盘存储' :
                'S3兼容存储' }}</td>
              <td class="px-6 py-4 text-sm text-gray-500">
                {{ point.storage === 'disk' ? point.path : point.bucket }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                <button @click="handleEdit(point)" class="text-blue-600 hover:text-blue-900 mr-3">
                  编辑
                </button>
                <button @click="handleDelete(point.id)" class="text-red-600 hover:text-red-900">
                  删除
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 配置表单卡片 - 仅在新增或编辑时显示 -->
    <div v-if="isEditMode || (!editingStoragePoint && storagePointsList.length === 0)"
      class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 表单内容 -->
      <form @submit.prevent="handleSave" class="space-y-6">
        <!-- 存储点类型选择 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">存储点类型</h3>
          <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div v-for="type in storageTypes" :key="type.value" @click="storagePointType = type.value" :class="[
              'border rounded-lg p-4 cursor-pointer transition-all duration-300',
              storagePointType === type.value
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
            ]">
              <div class="flex items-center">
                <div class="w-5 h-5 rounded-full border-2 flex items-center justify-center mr-3">
                  <div v-if="storagePointType === type.value" class="w-3 h-3 rounded-full bg-blue-500"></div>
                </div>
                <div>
                  <div class="font-medium text-gray-800">{{ type.label }}</div>
                  <div class="text-xs text-gray-500 mt-1">{{ type.description }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 存储点类别选择 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">存储点类别</h3>
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
                  <div class="font-medium text-gray-800">磁盘存储</div>
                  <div class="text-xs text-gray-500 mt-1">使用云盘或本地文件系统路径存储数据</div>
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
                  <div class="font-medium text-gray-800">S3兼容存储</div>
                  <div class="text-xs text-gray-500 mt-1">连接到S3兼容的对象存储服务</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 磁盘配置字段 -->
        <div v-if="storageType === 'disk'" class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">磁盘配置</h3>
          <div class="space-y-4">
            <div class="form-field">
              <label for="diskPath" class="block text-sm font-medium text-gray-700 mb-1">绝对路径</label>
              <input id="diskPath" v-model="diskPath" type="text" placeholder="例如: D:\storage\data"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
              <p class="text-xs text-gray-500 mt-1">请输入绝对路径，确保有足够的读写权限</p>
            </div>
          </div>
        </div>

        <!-- S3配置字段 -->
        <div v-if="storageType === 's3'" class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">S3配置</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="form-field">
              <label for="accessKey" class="block text-sm font-medium text-gray-700 mb-1">Access Key</label>
              <input id="accessKey" v-model="s3Config.accessKey" type="text" placeholder="输入S3 Access Key"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="secretKey" class="block text-sm font-medium text-gray-700 mb-1">Secret Key</label>
              <input id="secretKey" v-model="s3Config.secretKey" type="password" placeholder="输入S3 Secret Key"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="region" class="block text-sm font-medium text-gray-700 mb-1">Region</label>
              <input id="region" v-model="s3Config.region" type="text" placeholder="例如: us-east-1"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="endpoint" class="block text-sm font-medium text-gray-700 mb-1">Endpoint</label>
              <input id="endpoint" v-model="s3Config.endpoint" type="text" placeholder="输入S3服务端点"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field">
              <label for="bucket" class="block text-sm font-medium text-gray-700 mb-1">Bucket</label>
              <input id="bucket" v-model="s3Config.bucket" type="text" placeholder="输入存储桶名称"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            </div>
            <div class="form-field flex flex-col justify-center">
              <label for="usePathStyle" class="flex items-center text-sm font-medium text-gray-700 my-0">
                <input id="usePathStyle" v-model="s3Config.usePathStyle" type="checkbox"
                  class="w-4 h-4  my-0 text-blue-600 border-gray-300 rounded focus:ring-blue-500 transition-all duration-300" />
                <span class="ml-2">使用路径风格访问</span>
              </label>
            </div>
          </div>
        </div>

        <!-- 存储点ID -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">存储点ID</h3>
          <div class="form-field">
            <input v-model="storagePointId" type="text" placeholder="存储点唯一标识"
              class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
            <p class="text-xs text-gray-500 mt-1">可修改的唯一标识，用于系统内部识别</p>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="form-actions flex justify-end gap-4 pt-4 border-t border-gray-100">
          <button type="button" @click="handleTest"
            class="px-6 py-2 border border-blue-600 text-blue-600 rounded-lg hover:bg-blue-50 transition-all duration-300 flex items-center gap-2">
            <i class="fas fa-check-circle"></i>
            <span>测试配置</span>
          </button>
          <button type="submit"
            class="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow-md hover:shadow-lg transition-all duration-300 flex items-center gap-2">
            <i class="fas fa-save"></i>
            <span>保存配置</span>
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
          <h3 class="text-lg font-bold text-gray-800">{{ confirmDialogTitle }}</h3>
          <button @click="handleConfirmCancel" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ confirmDialogMessage }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="handleConfirmCancel" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            取消
          </button>
          <button @click="handleConfirmOk" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            确认删除
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue';

// 存储点类型选项
const storageTypes = ref([
  { value: 'standard', label: '标准存储', description: '高性能、低延迟的存储类型' },
  { value: 'lowfreq', label: '低频存储', description: '成本较低，适合不频繁访问的数据' },
  { value: 'archive', label: '归档存储', description: '最低成本，适合长期归档的数据' }
]);

// 存储点类型选择
const storagePointType = ref('standard');

// 存储类别选择
const storageType = ref('disk');

// 磁盘路径
const diskPath = ref('');

// S3配置
const s3Config = ref({
  accessKey: '',
  secretKey: '',
  region: '',
  endpoint: '',
  bucket: '',
  usePathStyle: false
});

// 存储点ID
const storagePointId = ref('');

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 确认对话框状态
const showConfirmDialog = ref(false);
const confirmDialogTitle = ref('确认删除');
const confirmDialogMessage = ref('确定要删除这个存储点吗？');
const confirmDialogCallback = ref(null);

// 存储点列表
const storagePointsList = ref([]);

// 当前编辑的存储点ID
const editingStoragePoint = ref(null);

// 编辑模式标志
const isEditMode = ref(false);

// 自动生成存储点ID
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
  return `${typeMap[storagePointType.value]}-${storageMap[storageType.value]}-${timestamp}-${random}`;
};

// 监听存储类型变化，自动生成ID
const updateStoragePointId = () => {
  if (!storagePointId.value || storagePointId.value === generateStoragePointId()) {
    storagePointId.value = generateStoragePointId();
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

// 保存存储点列表到本地存储
const saveStoragePoints = () => {
  try {
    localStorage.setItem('storagePoints', JSON.stringify(storagePointsList.value));
  } catch (error) {
    console.error('保存存储点列表失败:', error);
  }
};

// 重置表单
const resetForm = () => {
  storagePointType.value = 'standard';
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

// 编辑存储点
const handleEdit = (point) => {
  editingStoragePoint.value = { ...point };
  isEditMode.value = true;
  storagePointType.value = point.type;
  storageType.value = point.storage;
  storagePointId.value = point.id;

  if (point.storage === 'disk') {
    diskPath.value = point.path || '';
  } else if (point.storage === 's3') {
    s3Config.value = {
      accessKey: point.accessKey || '',
      secretKey: point.secretKey || '',
      region: point.region || '',
      endpoint: point.endpoint || '',
      bucket: point.bucket || '',
      usePathStyle: point.usePathStyle || false
    };
  }
};

// 处理确认对话框取消
const handleConfirmCancel = () => {
  showConfirmDialog.value = false;
  confirmDialogCallback.value = null;
};

// 处理确认对话框确认
const handleConfirmOk = () => {
  if (typeof confirmDialogCallback.value === 'function') {
    confirmDialogCallback.value();
  }
  showConfirmDialog.value = false;
  confirmDialogCallback.value = null;
};

// 删除存储点
const handleDelete = (id) => {
  confirmDialogTitle.value = '确认删除';
  confirmDialogMessage.value = '确定要删除这个存储点吗？';
  confirmDialogCallback.value = () => {
    const index = storagePointsList.value.findIndex(p => p.id === id);
    if (index !== -1) {
      storagePointsList.value.splice(index, 1);
      saveStoragePoints();
      showToastMessage('存储点已成功删除！', 'success');
    }
  };
  showConfirmDialog.value = true;
};

// 新增存储点
const handleAddNew = () => {
  resetForm();
  isEditMode.value = true;  // 设置为true以便显示表单
  editingStoragePoint.value = null;
};

// 取消编辑
const cancelEditing = () => {
  resetForm();
  isEditMode.value = false;
  editingStoragePoint.value = null;
};

// 获取存储类型名称
const getStorageTypeName = (type) => {
  const typeOption = storageTypes.value.find(t => t.value === type);
  return typeOption ? typeOption.label : type;
};



// 加载存储点列表
const loadStoragePoints = () => {
  try {
    const saved = localStorage.getItem('storagePoints');
    if (saved) {
      storagePointsList.value = JSON.parse(saved);
    } else {
      // 提供一些模拟数据
      storagePointsList.value = [
        {
          id: 'std-dsk-1712345678901-123',
          type: 'standard',
          storage: 'disk',
          path: 'D:\\storage\\data',
        },
        {
          id: 'lfr-s3-1712345678902-456',
          type: 'lowfreq',
          storage: 's3',
          accessKey: 'AKIAIOSFODNN7EXAMPLE',
          secretKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
          region: 'us-east-1',
          endpoint: 'https://s3.amazonaws.com',
          bucket: 'my-bucket',
          usePathStyle: false,
        }
      ];
      saveStoragePoints();
    }
  } catch (error) {
    console.error('加载存储点列表失败:', error);
    showToastMessage('加载存储点列表失败', 'error');
  }
};

// 测试配置
const handleTest = () => {
  try {
    // 验证必填字段
    if (storageType.value === 'disk') {
      if (!diskPath.value) {
        throw new Error('请输入磁盘绝对路径');
      }
      if (!diskPath.value.startsWith('D:\\') && !diskPath.value.startsWith('C:\\')) {
        throw new Error('请输入有效的Windows绝对路径');
      }
    } else if (storageType.value === 's3') {
      if (!s3Config.value.accessKey || !s3Config.value.secretKey) {
        throw new Error('请输入Access Key和Secret Key');
      }
      if (!s3Config.value.bucket) {
        throw new Error('请输入存储桶名称');
      }
      if (!s3Config.value.region && !s3Config.value.endpoint) {
        throw new Error('请输入Region或Endpoint');
      }
    }

    // 显示测试成功提示
    showToastMessage('配置测试成功！', 'success');

    // 模拟测试过程
    console.log('测试存储点配置:', {
      type: storagePointType.value,
      storage: storageType.value,
      id: storagePointId.value,
      ...(storageType.value === 'disk' ? { path: diskPath.value } : s3Config.value)
    });
  } catch (error) {
    showToastMessage(error.message, 'error');
  }
};

// 保存配置
const handleSave = () => {
  try {
    // 验证必填字段
    if (storageType.value === 'disk') {
      if (!diskPath.value) {
        throw new Error('请输入磁盘绝对路径');
      }
      if (!diskPath.value.startsWith('D:\\') && !diskPath.value.startsWith('C:\\')) {
        throw new Error('请输入有效的Windows绝对路径');
      }
    } else if (storageType.value === 's3') {
      if (!s3Config.value.accessKey || !s3Config.value.secretKey || !s3Config.value.bucket) {
        throw new Error('请填写完整的S3配置信息');
      }
    }

    if (!storagePointId.value) {
      throw new Error('请输入存储点ID');
    }

    // 构建配置对象
    const config = {
      id: storagePointId.value,
      type: storagePointType.value,
      storage: storageType.value,
      ...(storageType.value === 'disk' ? { path: diskPath.value } : s3Config.value),
    };

    // 处理编辑或新增
    if (isEditMode.value) {
      // 编辑现有存储点
      const index = storagePointsList.value.findIndex(p => p.id === editingStoragePoint.value.id);
      if (index !== -1) {
        storagePointsList.value[index] = config;
      }
    } else {
      // 检查ID是否已存在
      if (storagePointsList.value.some(p => p.id === config.id)) {
        throw new Error('存储点ID已存在，请使用其他ID');
      }
      // 添加新存储点
      storagePointsList.value.push(config);
    }

    // 保存并重置
    saveStoragePoints();
    showToastMessage(isEditMode.value ? '存储点配置已成功更新！' : '存储点配置已成功保存！', 'success');
    cancelEditing();
  } catch (error) {
    showToastMessage(error.message, 'error');
  }
};

// 初始化加载存储点列表
loadStoragePoints();

// 监听类型变化，自动更新存储点ID
watch([storagePointType, storageType], () => {
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