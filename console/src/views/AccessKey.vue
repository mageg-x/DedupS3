<template>
  <div class="access-keys-container">
    <!-- 头部搜索和创建按钮 -->
    <div class="access-keys-header flex items-center justify-between mb-6">
      <h2 class="text-2xl font-bold text-gray-800">{{ t('accessKey.pageTitle') }}</h2>
      <div class="header-actions flex items-center gap-4">
        <!-- 搜索框 -->
        <div class="relative">
          <input type="text" :placeholder="t('accessKey.searchPlaceholder')" v-model="searchQuery"
            class="search-input pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300 w-64" />
          <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
        </div>
        <!-- 创建按钮 -->
        <button @click="handleCreateAccessKey"
          class="create-button bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg shadow-md hover:shadow-lg transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-plus"></i>
          <span>{{ t('accessKey.createKey') }}</span>
        </button>
      </div>
    </div>

    <!-- 访问密钥列表 -->
    <div class="access-keys-list bg-white rounded-xl shadow-md overflow-hidden">
      <!-- 列表头部 -->
        <div class="list-header grid grid-cols-5 gap-4 px-6 py-4 border-b border-gray-100 bg-gray-50">
          <div class="flex items-center justify-center"></div>
          <div class="font-medium text-sm text-gray-700 flex items-center">{{ t('accessKey.accessKey') }}</div>
          <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('accessKey.isExpired') }}</div>
          <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('accessKey.isEnabled') }}</div>
          <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('common.operation') }}</div>
        </div>

      <!-- 列表内容 -->
      <div class="list-content">
        <!-- 空状态 -->
        <div v-if="paginatedKeys.length === 0"
          class="empty-state py-12 flex flex-col items-center justify-center text-center">
          <div class="empty-icon w-20 h-20 bg-gray-100 rounded-full flex items-center justify-center mb-4">
            <i class="fas fa-key text-3xl text-gray-400"></i>
          </div>
          <h3 class="text-lg font-medium text-gray-800 mb-2">{{ t('accessKey.noKeys') }}</h3>
          <p class="text-gray-500 mb-6">{{ t('accessKey.clickCreateKey') }}</p>
          <button @click="handleCreateAccessKey"
            class="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg shadow transition-all duration-300">
            {{ t('accessKey.createFirstKey') }}
          </button>
        </div>

        <!-- 密钥列表 -->
        <div v-for="key in paginatedKeys" :key="key.id" @click="selectKey(key.id)" :class="[
          'list-item grid grid-cols-5 gap-4 px-6 py-4 border-b border-gray-100 hover:bg-gray-50 transition-colors duration-200',
          selectedKeyId === key.id ? 'bg-blue-50' : ''
        ]">
          <!-- 单选框 -->
          <div class="flex items-center justify-center">
            <div
              class="w-4 h-4 rounded-full border border-gray-300 bg-white cursor-pointer flex items-center justify-center hover:border-blue-500 transition-colors"
              @click.stop="selectKey(key.id)">
              <div v-if="selectedKeyId === key.id" class="w-2 h-2 rounded-full bg-blue-500"></div>
            </div>
          </div>

          <!-- 访问密钥 -->
          <div class="flex items-center">
            <div class="font-medium text-xs text-gray-800 truncate min-w-fit">
              {{ key.accessKey }}
            </div>
          </div>

          <!-- 是否过期 -->
          <div class="flex items-center justify-center">
            <span :class="[
              'px-2 py-1 rounded-full text-xs font-medium',
              key.isExpired ? 'bg-red-100 text-red-600' : 'bg-green-100 text-green-600'
            ]">
              {{ key.isExpired ? t('accessKey.expired') : t('accessKey.notExpired') }}
            </span>
          </div>

          <!-- 是否启用 -->
          <div class="flex items-center justify-center">
            <span :class="[
              'px-2 py-1 rounded-full text-xs font-medium',
              key.enabled ? 'bg-green-100 text-green-600' : 'bg-gray-100 text-gray-600'
            ]">
              {{ key.enabled ? t('accessKey.enabled') : t('accessKey.disabled') }}
            </span>
          </div>

          <!-- 操作按钮 -->
          <div class="flex items-center justify-center gap-2">
            <button @click.stop="handleEditKey(key.id)"
              class="edit-button p-2 text-blue-500 hover:bg-blue-50 rounded-md transition-all duration-200" :title="t('common.edit')">
              <i class="fas fa-edit"></i>
            </button>
            <button @click.stop="handleDeleteKey(key.id)"
              class="delete-button p-2 text-red-500 hover:bg-red-50 rounded-md transition-all duration-200" :title="t('common.delete')">
              <i class="fas fa-trash-alt"></i>
            </button>
          </div>
        </div>
      </div>

      <!-- 分页 -->
      <div v-if="totalPages > 1"
        class="pagination px-6 py-4 flex items-center justify-between border-t border-gray-100 bg-gray-50">
        <div class="page-info text-sm text-gray-600">
          {{ t('common.totalRecords', { total: filteredKeys.length, size: pageSize }) }}
        </div>
        <div class="page-controls flex items-center gap-2">
          <button @click="currentPage = 1" :disabled="currentPage === 1"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
            <i class="fas fa-angle-double-left"></i>
          </button>
          <button @click="currentPage--" :disabled="currentPage === 1"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
            <i class="fas fa-angle-left"></i>
          </button>
          <span class="page-number px-3 py-1 text-sm font-medium text-gray-700">
            {{ currentPage }} / {{ totalPages }}
          </span>
          <button @click="currentPage++" :disabled="currentPage === totalPages"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
            <i class="fas fa-angle-right"></i>
          </button>
          <button @click="currentPage = totalPages" :disabled="currentPage === totalPages"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
            <i class="fas fa-angle-double-right"></i>
          </button>
        </div>
      </div>
    </div>

    <!-- 创建密钥对话框 -->
    <div v-if="dialogVisible"
      class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ editingKey ? t('accessKey.editAccessKey') : t('accessKey.createAccessKey') }}</h3>
          <button @click="resetForm" class="close-button text-gray-500 hover:text-gray-700 transition-colors"
            :aria-label="t('common.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body p-5">
          <div class="form-content space-y-4">
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.accessKey') }} </label>
              <input v-model="formData.accessKey" type="text" :placeholder="t('accessKey.enterAccessKey')"
                class="form-input w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none" :disabled="editingKey" />
            </div>
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.secretKey') }} </label>
              <input v-model="formData.secretKey" type="text" :placeholder="t('accessKey.enterSecretKey')"
                class="form-input w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none" :disabled="editingKey" />
            </div>
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.expiryDate') }} </label>
              <el-date-picker
                v-model="formData.expiresAt"
                type="date"
                :placeholder="t('accessKey.enterDate')"
                class="w-full"
              />
            </div>
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('common.status') }} </label>
              <div class="flex items-center space-x-4">
                <label class="inline-flex items-center">
                  <input type="radio" v-model="formData.enabled" :value="true"
                    class="form-radio h-4 w-4 text-blue-600 border-gray-300 focus:ring-blue-500" />
                  <span class="ml-2 text-sm text-gray-700">{{ t('accessKey.enabled') }}</span>
                </label>
                <label class="inline-flex items-center">
                  <input type="radio" v-model="formData.enabled" :value="false"
                    class="form-radio h-4 w-4 text-blue-600 border-gray-300 focus:ring-blue-500" />
                  <span class="ml-2 text-sm text-gray-700">{{ t('accessKey.disabled') }}</span>
                </label>
              </div>
            </div>
          </div>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="resetForm"
            class="cancel-button px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('common.cancel') }}
          </button>
          <button @click="handleSubmit"
            class="confirm-button px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ editingKey ? t('common.save') : t('common.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)]  flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('common.confirmDelete') }}</h3>
          <button @click="deleteDialogVisible = false" class="close-button text-gray-500 hover:text-gray-700 transition-colors" :aria-label="t('common.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body p-5">
          <p class="text-gray-700">{{ t('accessKey.confirmDeleteKey') }}</p>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="deleteDialogVisible = false" class="cancel-button px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('common.cancel') }}
          </button>
          <button @click="confirmDeleteKey" class="confirm-button px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('common.delete') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 密钥详情对话框 -->
    <div v-if="keyDetailsVisible"
      class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('accessKey.keyDetails') }}</h3>
          <button @click="keyDetailsVisible = false"
            class="close-button text-gray-500 hover:text-gray-700 transition-colors" :aria-label="t('common.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body p-5">
          <div class="key-details-content space-y-4">
            <div class="detail-item">
              <label class="detail-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.accessKeyId') }} </label>
              <div class="detail-value bg-gray-50 px-4 py-2 rounded-lg text-sm font-mono break-all">
                {{ newKey.accessKey }}
              </div>
            </div>
            <div class="detail-item">
              <label class="detail-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.secretAccessKey') }} </label>
              <div class="detail-value bg-gray-50 px-4 py-2 rounded-lg text-sm font-mono break-all">
                {{ newKey.secretKey }}
              </div>
            </div>
            <div class="warning-message p-3 bg-yellow-50 border-l-4 border-yellow-400 text-sm text-yellow-700 mt-4">
              <i class="fas fa-exclamation-triangle mr-2"></i>
              {{ t('accessKey.saveSecretKeyWarning') }}
            </div>
          </div>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end">
          <button @click="keyDetailsVisible = false"
            class="confirm-button px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ t('accessKey.iHaveSaved') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElDatePicker } from 'element-plus';

const { t } = useI18n();

// 选中的密钥ID
const selectedKeyId = ref(null);
const selectKey = (keyId) => {
  selectedKeyId.value = keyId === selectedKeyId.value ? null : keyId;
};

// 搜索
const searchQuery = ref('');

// 密钥数据
const accessKeys = ref([
  { id: '1', accessKey: 'AKIAIOSFODNN7EXAMPLE', createdAt: '2023-01-15T10:30:00Z', expiresAt: '2024-01-15T10:30:00Z', isExpired: false, enabled: true, name: 'Admin Key' },
  { id: '2', accessKey: 'AKIAI44QH8DHBEXAMPLE', createdAt: '2023-03-20T14:45:00Z', expiresAt: '2023-09-20T14:45:00Z', isExpired: true, enabled: false, name: 'Old Key' },
  { id: '3', accessKey: 'AKIAIXPMKX6O7EXAMPLE', createdAt: '2023-06-10T09:15:00Z', expiresAt: null, isExpired: false, enabled: true, name: 'No Expiry Key' },
  { id: '4', accessKey: 'AKIAIYZJO7EXAMPLE', createdAt: '2023-07-15T16:20:00Z', expiresAt: null, isExpired: false, enabled: true, name: 'Another Key' },
  { id: '5', accessKey: 'AKIAIYZJO7EXAMPLE', createdAt: '2023-07-15T16:20:00Z', expiresAt: null, isExpired: false, enabled: true, name: 'Another Key' },
  { id: '6', accessKey: 'AKIAIYZJO7EXAMPLE', createdAt: '2023-07-15T16:20:00Z', expiresAt: null, isExpired: false, enabled: true, name: 'Another Key' },
  { id: '7', accessKey: 'AKIAIYZJO7EXAMPLE', createdAt: '2023-07-15T16:20:00Z', expiresAt: null, isExpired: false, enabled: true, name: 'Another Key' },
  { id: '8', accessKey: 'AKIAIYZJO7EXAMPLE', createdAt: '2023-07-15T16:20:00Z', expiresAt: null, isExpired: false, enabled: true, name: 'Another Key' }
]);

// 分页
const currentPage = ref(1);
const pageSize = ref(6);

const filteredKeys = computed(() => {
  if (!searchQuery.value) return accessKeys.value;
  const query = searchQuery.value.toLowerCase();
  return accessKeys.value.filter(key => key.accessKey.toLowerCase().includes(query) || key.name.toLowerCase().includes(query));
});

const paginatedKeys = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value;
  return filteredKeys.value.slice(start, start + pageSize.value);
});

const totalPages = computed(() => Math.ceil(filteredKeys.value.length / pageSize.value));

// 对话框与表单
const dialogVisible = ref(false);
const keyDetailsVisible = ref(false);
const deleteDialogVisible = ref(false);
const editingKey = ref(null);
const keyToDelete = ref(null);
const formData = ref({ accessKey: '', secretKey: '', expiresAt: '', enabled: true });
const newKey = ref({ accessKey: '', secretKey: '' });

const handleCreateAccessKey = () => {
  resetForm();
  dialogVisible.value = true;
};

// 暂时移除编辑功能，因为用户需求只关注创建功能
const handleEditKey = (id) => {
    // 加载密钥数据到编辑表单
    const key = accessKeys.value.find(k => k.id === id);
    if (key) {
      editingKey.value = id;
      formData.value = {
        accessKey: key.accessKey,
        secretKey: key.secretKey || '',
        expiresAt: key.expiresAt ? key.expiresAt.split('T')[0] : '',
        enabled: key.enabled
      };
      dialogVisible.value = true;
    }
  };

const handleDeleteKey = (id) => {
  keyToDelete.value = id;
  deleteDialogVisible.value = true;
};

const confirmDeleteKey = () => {
  accessKeys.value = accessKeys.value.filter(k => k.id !== keyToDelete.value);
  if (paginatedKeys.value.length === 0 && currentPage.value > 1) currentPage.value--;
  deleteDialogVisible.value = false;
  keyToDelete.value = null;
};

const handleSubmit = () => {
    if (editingKey.value) {
      // 编辑现有密钥
      const index = accessKeys.value.findIndex(k => k.id === editingKey.value);
      if (index !== -1) {
        accessKeys.value[index] = {
          ...accessKeys.value[index],
          expiresAt: formData.value.expiresAt ? `${formData.value.expiresAt}T23:59:59Z` : null,
          isExpired: formData.value.expiresAt ? new Date(`${formData.value.expiresAt}T23:59:59Z`) < new Date() : false,
          enabled: formData.value.enabled
        };
      }
      dialogVisible.value = false;
      resetForm();
    } else {
      // 创建新的访问密钥
      const newAccessKey = {
        id: Date.now().toString(),
        accessKey: formData.value.accessKey,
        secretKey: formData.value.secretKey,
        name: formData.value.accessKey, // 使用accessKey作为名称
        createdAt: new Date().toISOString(),
        expiresAt: formData.value.expiresAt ? `${formData.value.expiresAt}T23:59:59Z` : null,
        isExpired: formData.value.expiresAt ? new Date(`${formData.value.expiresAt}T23:59:59Z`) < new Date() : false,
        enabled: formData.value.enabled
      };
      accessKeys.value.unshift(newAccessKey);
      newKey.value = { accessKey: formData.value.accessKey, secretKey: formData.value.secretKey };
      dialogVisible.value = false;
      setTimeout(() => (keyDetailsVisible.value = true), 300);
    }
  };

const resetForm = () => {
  editingKey.value = null;
  formData.value = { accessKey: '', secretKey: '', expiresAt: '', enabled: true };
  dialogVisible.value = false;
};

// 移除自动生成功能，因为现在用户需要手动输入Access Key和Secret Key

</script>

<style scoped>
/* 响应式 grid 布局替代 flex，确保列对齐 */
.list-header,
.list-item {
  display: grid;
  grid-template-columns: 0.5fr 2fr 1fr 1fr 1fr;
  gap: 1rem;
  align-items: center;
}

/* 移除旧的 flex 样式 */
@media (max-width: 768px) {

  .list-header,
  .list-item {
    grid-template-columns: 1fr;
    text-align: left;
  }

  .list-item>div {
    padding: 4px 0;
  }

  .list-item>div:nth-child(1) {
    display: flex;
    justify-content: flex-start;
    margin-bottom: 0.5rem;
  }
}
</style>