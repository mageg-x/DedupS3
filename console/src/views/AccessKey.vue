<template>
  <div class="access-keys-container">
    <!-- 头部搜索和创建按钮 -->
    <div class="access-keys-header flex items-center justify-between mb-6">
      <h2 class="text-2xl font-bold text-gray-800">{{ t('accessKey.pageTitle') }}</h2>
      <div class="header-actions flex items-center gap-4">
        <!-- 搜索框 -->
        <div class="relative">
          <input 
            type="text" 
            :placeholder="t('accessKey.searchPlaceholder')" 
            v-model="searchQuery"
            class="search-input pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300 w-64" 
          />
          <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
        </div>
        <!-- 创建按钮 -->
        <button 
          @click="handleCreateAccessKey"
          class="create-button bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg shadow-md hover:shadow-lg transition-all duration-300 flex items-center gap-2"
        >
          <i class="fas fa-plus"></i>
          <span>{{ t('accessKey.createKey') }}</span>
        </button>
      </div>
    </div>

    <!-- 访问密钥列表 -->
    <div class="access-keys-list bg-white rounded-xl shadow-md overflow-hidden">
      <!-- 列表头部 -->
      <div class="list-header grid grid-cols-6 gap-4 px-6 py-4 border-b border-gray-100 bg-gray-50">
        <div class="flex items-center justify-center"></div>
        <div class="font-medium text-sm text-gray-700 flex items-center">{{ t('accessKey.accessKey') }}</div>
        <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('accessKey.creater') }}</div>
        <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('accessKey.isExpired') }}</div>
        <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('accessKey.isEnabled') }}</div>
        <div class="font-medium text-sm text-gray-700 flex items-center justify-center">{{ t('common.operation') }}</div>
      </div>

      <!-- 列表内容 -->
      <div class="list-content">
        <!-- 空状态 -->
        <div v-if="paginatedKeys.length === 0" class="empty-state py-12 flex flex-col items-center justify-center text-center">
          <div class="empty-icon w-20 h-20 bg-gray-100 rounded-full flex items-center justify-center mb-4">
            <i class="fas fa-key text-3xl text-gray-400"></i>
          </div>
          <h3 class="text-lg font-medium text-gray-800 mb-2">{{ t('accessKey.noKeys') }}</h3>
          <p class="text-gray-500 mb-6">{{ t('accessKey.clickCreateKey') }}</p>
          <button 
            @click="handleCreateAccessKey"
            class="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg shadow transition-all duration-300"
          >
            {{ t('accessKey.createFirstKey') }}
          </button>
        </div>

        <!-- 密钥列表 -->
        <div v-if="loading" class="flex justify-center items-center p-8">
          <div class="text-gray-500">Loading...</div>
        </div>
        <div 
          v-else 
          v-for="key in paginatedKeys" 
          :key="key.id" 
          @click="selectKey(key.id)" 
          :class="[
            'list-item grid grid-cols-6 gap-4 px-6 py-4 border-b border-gray-100 hover:bg-gray-50 transition-colors duration-200',
            selectedKeyId === key.id ? 'bg-blue-50' : ''
          ]"
        >
          <!-- 单选框 -->
          <div class="flex items-center justify-center">
            <div
              class="w-4 h-4 rounded-full border border-gray-300 bg-white cursor-pointer flex items-center justify-center hover:border-blue-500 transition-colors"
              @click.stop="selectKey(key.id)"
            >
              <div v-if="selectedKeyId === key.id" class="w-2 h-2 rounded-full bg-blue-500"></div>
            </div>
          </div>

          <!-- 访问密钥 -->
          <div class="flex items-center">
            <div class="font-medium text-xs text-gray-800 truncate min-w-fit">
              {{ key.accessKey }}
            </div>
          </div>

          <!-- 创建者 -->
          <div class="flex items-center justify-center">
            <span class="text-sm text-gray-600">
              {{ key.creater || '-' }}
            </span>
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
            <button 
              @click.stop="handleEditKey(key.id)"
              class="edit-button p-2 text-blue-500 hover:bg-blue-50 rounded-md transition-all duration-200" 
              :title="t('common.edit')"
            >
              <i class="fas fa-edit"></i>
            </button>
            <button 
              @click.stop="handleDeleteKey(key.id)"
              class="delete-button p-2 text-red-500 hover:bg-red-50 rounded-md transition-all duration-200" 
              :title="t('common.delete')"
            >
              <i class="fas fa-trash-alt"></i>
            </button>
          </div>
        </div>
      </div>

      <!-- 分页 -->
      <div v-if="totalPages > 1" class="pagination px-6 py-4 flex items-center justify-between border-t border-gray-100 bg-gray-50">
        <div class="page-info text-sm text-gray-600">
          {{ t('common.totalRecords', { total: filteredKeys.length, size: pageSize }) }}
        </div>
        <div class="page-controls flex items-center gap-2">
          <button 
            @click="currentPage = 1" 
            :disabled="currentPage === 1"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <i class="fas fa-angle-double-left"></i>
          </button>
          <button 
            @click="currentPage--" 
            :disabled="currentPage === 1"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <i class="fas fa-angle-left"></i>
          </button>
          <span class="page-number px-3 py-1 text-sm font-medium text-gray-700">
            {{ currentPage }} / {{ totalPages }}
          </span>
          <button 
            @click="currentPage++" 
            :disabled="currentPage === totalPages"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <i class="fas fa-angle-right"></i>
          </button>
          <button 
            @click="currentPage = totalPages" 
            :disabled="currentPage === totalPages"
            class="page-button px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <i class="fas fa-angle-double-right"></i>
          </button>
        </div>
      </div>
    </div>

    <!-- 创建密钥对话框 -->
    <div v-if="dialogVisible" class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">
            {{ editingKey ? t('accessKey.editAccessKey') : t('accessKey.createAccessKey') }}
          </h3>
          <button @click="resetForm" class="close-button text-gray-500 hover:text-gray-700 transition-colors" :aria-label="t('common.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body p-5">
          <div class="form-content space-y-4">
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.accessKey') }} </label>
              <input 
                v-model="formData.accessKey" 
                type="text" 
                :placeholder="t('accessKey.enterAccessKey')"
                class="form-input w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none" 
                :readonly="editingKey" 
              />
            </div>
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.secretKey') }} </label>
              <div class="relative">
                <input 
                  v-model="formData.secretKey" 
                  :type="showPassword ? 'text' : 'password'" 
                  :placeholder="t('accessKey.enterSecretKey')"
                  class="form-input w-full px-4 py-2 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none" 
                />
                <button 
                  type="button" 
                  @click="showPassword = !showPassword" 
                  class="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600 focus:outline-none"
                  :aria-label="showPassword ? t('accessKey.hidePassword') : t('accessKey.showPassword')"
                >
                  <i :class="showPassword ? 'fas fa-eye-slash' : 'fas fa-eye'" />
                </button>
              </div>
              <p v-if="editingKey" class="mt-1 text-xs text-gray-500">* 输入新的密钥内容进行修改，留空则保持不变</p>
            </div>
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.expiryDate') }} </label>
              <el-date-picker
                v-model="formData.expiresAt"
                type="date"
                value-format="YYYY-MM-DD"
                :placeholder="t('accessKey.enterDate')"
                class="w-full"
              />
            </div>
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('common.status') }} </label>
              <div class="flex items-center space-x-4">
                <label class="inline-flex items-center">
                  <input 
                    type="radio" 
                    v-model="formData.enabled" 
                    :value="true"
                    class="form-radio h-4 w-4 text-blue-600 border-gray-300 focus:ring-blue-500" 
                  />
                  <span class="ml-2 text-sm text-gray-700">{{ t('accessKey.enabled') }}</span>
                </label>
                <label class="inline-flex items-center">
                  <input 
                    type="radio" 
                    v-model="formData.enabled" 
                    :value="false"
                    class="form-radio h-4 w-4 text-blue-600 border-gray-300 focus:ring-blue-500" 
                  />
                  <span class="ml-2 text-sm text-gray-700">{{ t('accessKey.disabled') }}</span>
                </label>
              </div>
            </div>
          </div>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button 
            @click="resetForm"
            class="cancel-button px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors"
          >
            {{ t('common.cancel') }}
          </button>
          <button 
            @click="handleSubmit"
            class="confirm-button px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            {{ editingKey ? t('common.save') : t('common.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
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
          <button 
            @click="deleteDialogVisible = false" 
            class="cancel-button px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors"
          >
            {{ t('common.cancel') }}
          </button>
          <button 
            @click="confirmDeleteKey" 
            class="confirm-button px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
          >
            {{ t('common.delete') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 密钥详情对话框 -->
    <div v-if="keyDetailsVisible" class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('accessKey.keyDetails') }}</h3>
          <button 
            @click="keyDetailsVisible = false"
            class="close-button text-gray-500 hover:text-gray-700 transition-colors" 
            :aria-label="t('common.close')"
          >
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
              <label class="detail-label block text-sm font-medium text-gray-700 mb-1"> {{ t('accessKey.secretKey') }} </label>
              <div class="detail-value bg-gray-50 px-4 py-2 rounded-lg text-sm font-mono break-all">
                ******** (密钥内容已隐藏) 
              </div>
            </div>
            <div class="warning-message p-3 bg-yellow-50 border-l-4 border-yellow-400 text-sm text-yellow-700 mt-4">
              <i class="fas fa-exclamation-triangle mr-2"></i>
              {{ t('accessKey.saveSecretKeyWarning') }}
            </div>
          </div>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end">
          <button 
            @click="keyDetailsVisible = false"
            class="confirm-button px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            {{ t('accessKey.iHaveSaved') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElDatePicker, ElMessage } from 'element-plus';
import { listaccesskey, createaccesskey, setaccesskey, delaccesskey } from '../api/admin.js';

// 国际化
const { t } = useI18n();

// 选中的密钥ID
const selectedKeyId = ref(null);
const selectKey = (keyId) => {
  selectedKeyId.value = keyId === selectedKeyId.value ? null : keyId;
};

// 搜索
const searchQuery = ref('');

// 数据加载
const accessKeys = ref([]);
const loading = ref(false);

// 加载访问密钥列表
const loadAccessKeys = async () => {
  loading.value = true;
  try {
    const res = await listaccesskey();
    if (res.success !== false) {
      // 转换 API 返回的数据格式
      accessKeys.value = res.data.map(item => ({
        id: item.accessKeyId,
        accessKey: item.accessKeyId,
        secretKey: item.secretKey,
        creater: item.creater,
        enabled: item.enable,
        expiresAt: item.expiredAt ? new Date(item.expiredAt).toISOString() : null,
        isExpired: item.expiredAt ? new Date(item.expiredAt) < new Date() : false
      }));
    } else {
      ElMessage.error(res.msg || 'Failed to load access keys');
    }
  } catch (error) {
    ElMessage.error('Failed to load access keys');
    console.error('Error loading access keys:', error);
  } finally {
    loading.value = false;
  }
};

// 分页
const currentPage = ref(1);
const pageSize = ref(6);

const filteredKeys = computed(() => {
  if (!searchQuery.value) return accessKeys.value;
  const query = searchQuery.value.toLowerCase();
  return accessKeys.value.filter(key => 
    key.accessKey.toLowerCase().includes(query) || 
    (key.name && key.name.toLowerCase().includes(query))
  );
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
const showPassword = ref(false);

// 生成随机字符串的辅助函数
const generateRandomString = (length) => {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  const charsLength = chars.length;
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * charsLength));
  }
  return result;
};

// 操作处理
const handleCreateAccessKey = () => {
  resetForm();
  // 默认生成20位AccessKey和40位secretKey
  formData.value.accessKey = generateRandomString(20);
  formData.value.secretKey = generateRandomString(40);
  // 默认设置过期时间为1年后
  const nextYear = new Date();
  nextYear.setFullYear(nextYear.getFullYear() + 1);
  formData.value.expiresAt = nextYear.toISOString().split('T')[0];
  dialogVisible.value = true;
};

const handleEditKey = (id) => {
  // 加载密钥数据到编辑表单
  const key = accessKeys.value.find(k => k.id === id);
  if (key) {
    editingKey.value = id;
    formData.value = {
      accessKey: key.accessKey,
      secretKey: key.secretKey || '', // 加载密钥内容，但通过password输入框隐藏明文
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

const confirmDeleteKey = async () => {
  try {
    const res = await delaccesskey({ accessKey: keyToDelete.value });
    if (res.success !== false) {
      ElMessage.success('Access key deleted successfully');
      await loadAccessKeys(); // 重新加载列表
    } else {
      ElMessage.error(res.msg || 'Failed to delete access key');
    }
  } catch (error) {
    ElMessage.error('Failed to delete access key');
    console.error('Error deleting access key:', error);
  } finally {
    deleteDialogVisible.value = false;
    keyToDelete.value = null;
  }
};

const handleSubmit = async () => {
  try {
    // 构建请求数据
    const requestData = {
      accessKey: formData.value.accessKey,
      enabled: formData.value.enabled,
      // 转换日期格式 - 更健壮的处理方式
      expiredAt: formData.value.expiresAt && formData.value.expiresAt !== '' 
        ? new Date(`${formData.value.expiresAt}T23:59:59Z`).toISOString() 
        : null
    };
    
    // 只有当secretKey有值时才添加到请求数据中（编辑模式下可以选择不修改）
    if (formData.value.secretKey !== '') {
      requestData.secretKey = formData.value.secretKey;
    }

    let res;
    if (editingKey.value) {
      // 编辑现有密钥
      res = await setaccesskey(requestData);
    } else {
      // 创建新的访问密钥
      res = await createaccesskey(requestData);
    }

    if (res?.success !== false) {
      ElMessage.success(editingKey.value ? 'Access key updated successfully' : 'Access key created successfully');
      dialogVisible.value = false;
      
      // 如果是创建新密钥，显示详情对话框
      if (!editingKey.value) {
        // 保存表单数据到临时变量，因为resetForm会清除它
        const tempFormData = {
          accessKey: formData.value.accessKey,
          secretKey: formData.value.secretKey
        };
        
        // 从API响应中获取实际生成的密钥，增加更多的数据路径检查
        console.log('API response data:', res); // 添加调试日志
        newKey.value = {
          accessKey: res?.accessKeyId || res?.data?.accessKeyId || res?.data?.accessKey || tempFormData.accessKey || '',
          secretKey: res?.secretKey || res?.data?.secretKey || res?.data?.secretKey || tempFormData.secretKey || ''
        };
        
        // 立即显示密钥详情对话框
        keyDetailsVisible.value = true;
        
        // 延迟重置表单和重新加载列表
        setTimeout(() => {
          resetForm();
          loadAccessKeys();
        }, 1000);
      } else {
        resetForm();
        await loadAccessKeys(); // 重新加载列表
      }
    } else {
      ElMessage.error(res.msg || (editingKey.value ? 'Failed to update access key' : 'Failed to create access key'));
    }
  } catch (error) {
    ElMessage.error(editingKey.value ? 'Failed to update access key' : 'Failed to create access key');
    console.error('Error submitting access key:', error);
  }
};

const resetForm = () => {
  editingKey.value = null;
  formData.value = { accessKey: '', secretKey: '', expiresAt: '', enabled: true };
  showPassword.value = false;
  dialogVisible.value = false;
};

// 组件挂载时加载数据
onMounted(() => {
  loadAccessKeys();
});
</script>

<style scoped>
/* 响应式 grid 布局替代 flex，确保列对齐 */
.list-header,
.list-item {
  display: grid;
  grid-template-columns: 0.5fr 2fr 1fr 1fr 1fr 1fr;
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

  .list-item > div {
    padding: 4px 0;
  }

  .list-item > div:nth-child(1) {
    display: flex;
    justify-content: flex-start;
    margin-bottom: 0.5rem;
  }
}
</style>