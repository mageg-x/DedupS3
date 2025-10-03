<template>
  <div class="quota-container">
    <!-- 头部标题 -->
    <div class="quota-header flex justify-between items-center px-4 mb-6">
      <div>
        <h2 class="text-2xl font-bold text-gray-800">配额管理</h2>
        <p class="text-gray-500 mt-2">
          超级管理员对各个主账号配置存储空间大小限制、对象个数限制，以及配置是否生效
        </p>
      </div>

      <!-- 操作按钮 -->
      <div class="flex gap-3">
        <button v-if="!isEditMode" @click="handleAddNew"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-plus"></i>
          <span>新增配额</span>
        </button>
        <button v-else @click="cancelEditing"
          class="px-4 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-times"></i>
          <span>取消编辑</span>
        </button>
      </div>
    </div>

    <!-- 配额列表卡片 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 空状态提示 -->
      <p v-if="quotaList.length === 0" class="text-gray-500 mb-4">
        暂无配额配置，点击上方按钮新增
      </p>

      <!-- 列表表格 -->
      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">账号ID</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">存储空间限制</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">对象个数限制</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">状态</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">操作</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="quota in quotaList" :key="quota.accountId">
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{{ quota.accountId }}</td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatLimit(quota.storageLimit, 'GB') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatLimit(quota.objectLimit) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="[
                  'px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full',
                  quota.enabled ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'
                ]">
                  {{ quota.enabled ? '已启用' : '已禁用' }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                <button @click="handleEdit(quota)" class="text-blue-600 hover:text-blue-900 mr-3">编辑</button>
                <button @click="handleDelete(quota.accountId)" class="text-red-600 hover:text-red-900">删除</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 编辑/新增表单 -->
    <div v-if="isEditMode || quotaList.length === 0" class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <form @submit.prevent="handleSave" class="space-y-6">
        <!-- 账号信息 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">账号信息</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="form-field">
              <label for="accountId" class="block text-sm font-medium text-gray-700 mb-1">账号ID</label>
              <input id="accountId" v-model="form.accountId" type="text" placeholder="输入主账号唯一标识"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
              <p class="text-xs text-gray-500 mt-1">主账号的唯一标识符</p>
            </div>
          </div>
        </div>

        <!-- 配额配置 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">配额配置</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="form-field">
              <label for="storageLimit" class="block text-sm font-medium text-gray-700 mb-1">存储空间限制 (GB)</label>
              <input id="storageLimit" v-model.number="form.storageLimit" type="number" min="0"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
              <p class="text-xs text-gray-500 mt-1">设置为0表示无限制</p>
            </div>
            <div class="form-field">
              <label for="objectLimit" class="block text-sm font-medium text-gray-700 mb-1">对象个数限制</label>
              <input id="objectLimit" v-model.number="form.objectLimit" type="number" min="0"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
              <p class="text-xs text-gray-500 mt-1">设置为0表示无限制</p>
            </div>
          </div>
        </div>

        <!-- 状态配置 -->
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">状态配置</h3>
          <div class="form-field flex items-center">
            <label for="enabled" class="flex items-center text-sm font-medium text-gray-700 cursor-pointer">
              <input id="enabled" v-model="form.enabled" type="checkbox"
                class="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-blue-500 transition-all duration-300" />
              <span class="ml-2">启用此配额配置</span>
            </label>
          </div>
        </div>

        <!-- 提交按钮 -->
        <div class="form-actions pt-4 border-t border-gray-100 flex justify-end gap-4">
          <button type="submit"
            class="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow-md hover:shadow-lg transition-all duration-300 flex items-center gap-2">
            <i class="fas fa-save"></i>
            <span>{{ isEditMode && editingQuota ? '更新配置' : '保存配置' }}</span>
          </button>
        </div>
      </form>
    </div>

    <!-- 提示框 Toast -->
    <transition name="fade-slide">
      <div v-if="showToast" class="toast" :class="[toastType === 'success' ? 'bg-green-500' : 'bg-red-500']">
        <div class="flex items-center gap-2">
          <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
          <i v-else class="fas fa-exclamation-circle"></i>
          <span>{{ toastMessage }}</span>
        </div>
      </div>
    </transition>

    <!-- 自定义确认对话框 -->
    <transition name="fade-slide">
      <div v-if="showConfirmDialog" class="confirm-dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
        <div class="confirm-dialog bg-white rounded-xl shadow-xl p-6 max-w-md w-full mx-4 transform transition-all">
          <div class="text-center mb-4">
            <h3 class="text-lg font-semibold text-gray-800 mb-2">{{ confirmDialogTitle }}</h3>
            <p class="text-gray-600">{{ confirmDialogMessage }}</p>
          </div>
          <div class="flex justify-center gap-4 mt-6">
            <button @click="handleConfirmCancel" class="px-5 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-all duration-300">
              取消
            </button>
            <button @click="handleConfirmOk" class="px-5 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg shadow transition-all duration-300">
              确认
            </button>
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';

// ========== 类型定义 ==========
interface Quota {
  accountId: string;
  storageLimit: number;
  objectLimit: number;
  enabled: boolean;
}

// ========== 响应式状态 ==========
const quotaList = ref < Quota[] > ([]);

const isEditMode = ref(false);
const editingQuota = ref < Quota | null > (null);

// 表单数据集中管理
const form = ref < Quota > ({
  accountId: '',
  storageLimit: 0,
  objectLimit: 0,
  enabled: true,
});

// Toast 提示
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref < 'success' | 'error' > ('success');

// 确认对话框状态
const showConfirmDialog = ref(false);
const confirmDialogTitle = ref('');
const confirmDialogMessage = ref('');
let confirmDialogCallback = null;

// ========== 工具函数 ==========
/**
 * 格式化限制值显示
 */
const formatLimit = (value: number, unit = ''): string => {
  return value > 0 ? `${value.toLocaleString()} ${unit}`.trim() : '无限制';
};

/**
 * 显示提示消息
 */
const showToastMessage = (message: string, type: 'success' | 'error' = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

/**
 * 保存到 localStorage
 */
const saveToStorage = () => {
  try {
    localStorage.setItem('quotas', JSON.stringify(quotaList.value));
  } catch (err) {
    console.error('持久化失败:', err);
    showToastMessage('保存失败：本地存储已满或受限', 'error');
  }
};

/**
 * 从 localStorage 加载
 */
const loadFromStorage = (): Quota[] => {
  try {
    const saved = localStorage.getItem('quotas');
    return saved ? JSON.parse(saved) : null;
  } catch (err) {
    console.warn('加载失败，使用默认数据:', err);
    return null;
  }
};

// ========== 业务逻辑 ==========
const resetForm = () => {
  form.value = {
    accountId: '',
    storageLimit: 0,
    objectLimit: 0,
    enabled: true,
  };
};

const handleAddNew = () => {
  resetForm();
  isEditMode.value = true;
  editingQuota.value = null;
};

const cancelEditing = () => {
  resetForm();
  isEditMode.value = false;
  editingQuota.value = null;
};

const handleEdit = (quota: Quota) => {
  editingQuota.value = { ...quota };
  Object.assign(form.value, quota);
  isEditMode.value = true;
};

const handleDelete = (id: string) => {
  // 显示自定义确认对话框
  confirmDialogTitle.value = '确认删除';
  confirmDialogMessage.value = '确定要删除这个配额配置吗？';
  confirmDialogCallback = () => {
    const index = quotaList.value.findIndex(q => q.accountId === id);
    if (index !== -1) {
      quotaList.value.splice(index, 1);
      saveToStorage();
      showToastMessage('配额配置已成功删除！');
    }
  };
  showConfirmDialog.value = true;
};

// 处理确认对话框取消
const handleConfirmCancel = () => {
  showConfirmDialog.value = false;
  confirmDialogCallback = null;
};

// 处理确认对话框确认
const handleConfirmOk = () => {
  if (typeof confirmDialogCallback === 'function') {
    confirmDialogCallback();
  }
  showConfirmDialog.value = false;
  confirmDialogCallback = null;
};

const handleSave = () => {
  try {
    const { accountId, storageLimit, objectLimit } = form.value;

    // 验证
    if (!accountId.trim()) throw new Error('请输入账号ID');
    if (storageLimit < 0) throw new Error('存储空间限制不能为负数');
    if (objectLimit < 0) throw new Error('对象个数限制不能为负数');

    const trimmedId = accountId.trim();

    if (isEditMode.value && editingQuota.value) {
      // 更新
      const idx = quotaList.value.findIndex(q => q.accountId === editingQuota.value!.accountId);
      if (idx !== -1) {
        quotaList.value[idx] = { ...form.value, accountId: trimmedId };
      }
    } else {
      // 新增
      if (quotaList.value.some(q => q.accountId === trimmedId)) {
        throw new Error('账号ID已存在，请使用其他ID');
      }
      quotaList.value.push({ ...form.value, accountId: trimmedId });
    }

    saveToStorage();
    showToastMessage(isEditMode.value ? '配额配置已更新！' : '配额配置已添加！');
    cancelEditing();
  } catch (err: any) {
    showToastMessage(err.message, 'error');
  }
};

// ========== 初始化 ==========
onMounted(() => {
  const savedData = loadFromStorage();

  if (savedData) {
    quotaList.value = savedData;
  } else {
    // 默认模拟数据
    quotaList.value = [
      { accountId: 'admin001', storageLimit: 0, objectLimit: 0, enabled: true },
      { accountId: 'user001', storageLimit: 100, objectLimit: 100000, enabled: true },
      { accountId: 'user002', storageLimit: 500, objectLimit: 500000, enabled: false },
    ];
    saveToStorage();
  }
});
</script>

<style scoped>
.quota-container {
  background-color: #f5f7fa;
  min-height: calc(100vh - 120px);
  padding: 2rem;
}

.config-card {
  border-radius: 12px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
  transition: all 0.3s ease;
}

.config-card:hover {
  box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
}

.form-section h3 {
  font-size: 1.1rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 1rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid #f0f0f0;
}

/* Toast 动画 */
.toast {
  position: fixed;
  top: 1rem;
  right: 1rem;
  padding: 0.75rem 1rem;
  border-radius: 0.5rem;
  color: white;
  box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
  z-index: 50;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  animation: slide-in 0.3s ease-out;
}

.fade-slide-enter-active,
.fade-slide-leave-active {
  transition: opacity 0.3s, transform 0.3s;
}

.fade-slide-enter-from,
.fade-slide-leave-to {
  opacity: 0;
  transform: translateX(20px);
}

@keyframes slide-in {
  from {
    opacity: 0;
    transform: translateX(20px);
  }

  to {
    opacity: 1;
    transform: translateX(0);
  }
}

/* 响应式 */
@media (max-width: 768px) {
  .quota-container {
    padding: 1rem;
  }
}
</style>