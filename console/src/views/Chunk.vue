<template>
  <div class="chunk-config-container">
    <!-- 头部标题和说明 -->
    <div class="header-section mb-6">
      <h2 class="page-title">{{ t('chunk.pageTitle') }}</h2>
      <p class="page-description">{{ t('chunk.pageDescription') }}</p>
    </div>

    <!-- 配置列表 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <div v-if="configs.length === 0" class="empty-state">
        <p class="text-gray-500">{{ t('chunk.noConfigs') }}</p>
      </div>

      <div v-else class="table-container">
        <table class="config-table">
          <thead>
            <tr>
              <th class="col-header">{{ t('chunk.storageID') }}</th>
              <th class="col-header">{{ t('chunk.chunkLength') }}</th>
              <th class="col-header">{{ t('chunk.fixedLength') }}</th>
              <th class="col-header">{{ t('chunk.encryption') }}</th>
              <th class="col-header">{{ t('chunk.compression') }}</th>
              <th class="col-header text-right">{{ t('chunk.operation') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="config in configs" :key="config.storageID" class="table-row">
              <td class="cell-data">{{ config.storageID }}</td>
              <td class="cell-data">{{ formatStorage(config.chunkSize) }}</td>
              <td class="cell-data">
                <span class="status-badge" :class="config.fixSize ? 'enabled' : 'disabled'">
                  {{ config.fixSize ? t('chunk.enabled') : t('chunk.disabled') }}
                </span>
              </td>
              <td class="cell-data">
                <span class="status-badge" :class="config.encrypt ? 'enabled' : 'disabled'">
                  {{ config.encrypt ? t('chunk.enabled') : t('chunk.disabled') }}
                </span>
              </td>
              <td class="cell-data">
                <span class="status-badge" :class="config.compress ? 'enabled' : 'disabled'">
                  {{ config.compress ? t('chunk.enabled') : t('chunk.disabled') }}
                </span>
              </td>
              <td class="cell-data text-right">
                <button @click="startEditing(config)" class="edit-btn">
                  <i class="fas fa-edit mr-1"></i>{{ t('chunk.edit') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 编辑表单 -->
    <div v-if="isEditing" class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <form @submit.prevent="saveConfig" class="edit-form">
        <div class="form-section">
          <h3 class="form-title">{{ t('chunk.editConfig') }}</h3>

          <div class="form-fields">
            <div class="form-field">
              <label class="form-label">{{ t('chunk.storageID') }}</label>
              <input
                v-model="editingConfig.storageID"
                type="text"
                disabled
                class="disabled-input"
              />
            </div>

            <div class="form-field">
              <label class="form-label">{{ t('chunk.chunkLength') }}</label>
              <div class="size-input-group">
                <input
                  v-model.number="editingConfig.chunkSizeDisplay"
                  type="number"
                  min="1"
                  max="1024"
                  class="size-input"
                />
                <select
                  v-model="editingConfig.chunkSizeUnit"
                  class="unit-select"
                >
                  <option value="KB">KB</option>
                  <option value="MB">MB</option>
                  <option value="GB">GB</option>
                </select>
              </div>
              <p class="input-hint">{{ t('chunk.recommendedValue') }}</p>
            </div>

            <div class="switch-group">
              <div class="switch-item">
                <label class="switch-label">
                  <span>{{ t('chunk.useFixedLengthChunking') }}</span>
                  <el-switch v-model="editingConfig.fixSize" active-color="#409EFF" inactive-color="#DCDFE6" />
                </label>
              </div>

              <div class="switch-item">
                <label class="switch-label">
                  <span>{{ t('chunk.enableDataEncryption') }}</span>
                  <el-switch v-model="editingConfig.encrypt" active-color="#409EFF" inactive-color="#DCDFE6" />
                </label>
              </div>

              <div class="switch-item">
                <label class="switch-label">
                  <span>{{ t('chunk.enableDataCompression') }}</span>
                  <el-switch v-model="editingConfig.compress" active-color="#409EFF" inactive-color="#DCDFE6" />
                </label>
              </div>
            </div>
          </div>
        </div>

        <div class="form-actions">
          <button type="button" @click="cancelEdit" class="cancel-btn">
            {{ t('chunk.cancel') }}
          </button>
          <button type="submit" class="save-btn">
            {{ t('chunk.saveConfiguration') }}
          </button>
        </div>
      </form>
    </div>

    <!-- 提示消息 -->
    <div v-if="showToast" :class="['toast', toastType]">
      <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
      <i v-else class="fas fa-exclamation-circle"></i>
      <span>{{ toastMessage }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { listchunkcfg, getchunkcfg, setchunkcfg } from '../api/admin.js';

// ==================== 翻译 ====================
const { t } = useI18n();

// ==================== 响应式状态 ====================
const configs = ref([]);
const isEditing = ref(false);
const editingConfig = ref({
  storageID: '',
  chunkSize: 0,
  chunkSizeDisplay: 32,
  chunkSizeUnit: 'MB',
  fixSize: true,
  encrypt: false,
  compress: true
});

const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// ==================== 工具函数 ====================
const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;
  setTimeout(() => (showToast.value = false), 3000);
};

const convertToDisplayUnit = (kbValue) => {
  if (kbValue === 0) return { value: 0, unit: 'KB' };
  const units = ['KB', 'MB', 'GB'];
  let value = kbValue;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return { value: parseFloat(value.toFixed(2)), unit: units[unitIndex] };
};

const convertToKB = (displayValue, unit) => {
  if (displayValue === 0) return 0;
  const multipliers = { KB: 1, MB: 1024, GB: 1024 ** 2 };
  return Math.round(displayValue * multipliers[unit]);
};

const formatStorage = (kbValue) => {
  const { value, unit } = convertToDisplayUnit(kbValue);
  return `${value} ${unit}`;
};

// ==================== API 调用 ====================
const loadConfigs = async () => {
  try {
    const result = await listchunkcfg();
    if (result.code === 0 && result.data) {
      const configsArray = [];
      for (const [storageID, config] of Object.entries(result.data)) {
        configsArray.push({
          storageID,
          chunkSize: config.ChunkSize || 32768,
          fixSize: config.FixSize ?? true,
          encrypt: config.Encrypt ?? false,
          compress: config.Compress ?? true
        });
      }
      configs.value = configsArray;
    } else {
      showToastMessage(result.msg || '加载配置列表失败', 'error');
      configs.value = [];
    }
  } catch (error) {
    showToastMessage('网络错误，加载配置列表失败', 'error');
    console.error('加载配置列表失败:', error);
    configs.value = [];
  }
};

const loadConfigDetail = async (storageID) => {
  try {
    const result = await getchunkcfg({ storageID });
    if (result.code === 0 && result.data) {
      const { chunkSize, fixSize, encrypt, compress } = result.data;
      const { value, unit } = convertToDisplayUnit(chunkSize);
      
      editingConfig.value = {
        storageID: result.data.storageID,
        chunkSize,
        chunkSizeDisplay: value,
        chunkSizeUnit: unit,
        fixSize: fixSize ?? true,
        encrypt: encrypt ?? false,
        compress: compress ?? true
      };
    } else {
      throw new Error(result.msg || '获取配置详情失败');
    }
  } catch (error) {
    showToastMessage(error.message, 'error');
    console.error('获取配置详情失败:', error);
  }
};

const saveConfig = async () => {
  try {
    if (!editingConfig.value.chunkSizeDisplay || editingConfig.value.chunkSizeDisplay <= 0) {
      throw new Error('请输入有效的切片长度');
    }

    const chunkSizeValue = convertToKB(
      editingConfig.value.chunkSizeDisplay, 
      editingConfig.value.chunkSizeUnit
    );

    const config = {
      storageID: editingConfig.value.storageID,
      chunkSize: chunkSizeValue,
      fixSize: editingConfig.value.fixSize,
      encrypt: editingConfig.value.encrypt,
      compress: editingConfig.value.compress
    };

    const result = await setchunkcfg(config);
    if (result.code === 0) {
      showToastMessage(t('chunk.saveSuccess'), 'success');
      await loadConfigs();
      cancelEdit();
    } else {
      showToastMessage(result.msg || '保存失败', 'error');
    }
  } catch (error) {
    showToastMessage(error.message, 'error');
  }
};

// ==================== UI 控制 ====================
const startEditing = async (config) => {
  await loadConfigDetail(config.storageID);
  isEditing.value = true;
};

const cancelEdit = () => {
  isEditing.value = false;
  editingConfig.value = {
    storageID: '',
    chunkSize: 0,
    chunkSizeDisplay: 32,
    chunkSizeUnit: 'MB',
    fixSize: true,
    encrypt: false,
    compress: true
  };
};

// ==================== 生命周期 ====================
onMounted(loadConfigs);
</script>

<style scoped>
/* 容器样式 */
.chunk-config-container {
  background-color: #f5f7fa;
  min-height: calc(100vh - 120px);
  padding: 2rem;
  max-width: 1200px;
  margin: 0 auto;
}

/* 头部样式 */
.header-section {
  margin-bottom: 24px;
}

.page-title {
  font-size: 1.5rem;
  font-weight: bold;
  color: #1f2937;
}

.page-description {
  color: #6b7280;
  margin-top: 0.5rem;
}

/* 卡片样式 */
.config-card {
  background: white;
  border-radius: 12px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
  padding: 24px;
  margin-bottom: 24px;
  transition: all 0.3s ease;
}

.config-card:hover {
  box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
}

/* 表格样式 */
.table-container {
  overflow-x: auto;
}

.config-table {
  width: 100%;
  border-collapse: collapse;
}

.col-header {
  padding: 0.75rem 1rem;
  text-align: left;
  font-size: 0.875rem;
  font-weight: 600;
  color: #6b7280;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  background-color: transparent;
}

.table-row {
  transition: background-color 0.2s;
}

.table-row:hover {
  background-color: #f9fafb;
}

.cell-data {
  padding: 1rem;
  font-size: 0.875rem;
  color: #4b5563;
  border-top: 1px solid #e5e7eb;
}

/* 状态标签样式 */
.status-badge {
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  font-weight: 600;
  border-radius: 0.5rem;
}

.status-badge.enabled {
  background-color: #dcfce7;
  color: #166534;
}

.status-badge.disabled {
  background-color: #fee2e2;
  color: #991b1b;
}

/* 编辑按钮样式 */
.edit-btn {
  color: #2563eb;
  cursor: pointer;
  transition: color 0.2s;
}

.edit-btn:hover {
  color: #1d4ed8;
}

/* 空状态样式 */
.empty-state {
  text-align: center;
  padding: 3rem;
}

/* 表单样式 */
.edit-form {
  space-y: 1.5rem;
}

.form-section {
  margin-bottom: 1.5rem;
}

.form-title {
  font-size: 1.125rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 1rem;
}

.form-fields {
  space-y: 1rem;
}

.form-field {
  margin-bottom: 1rem;
}

.form-label {
  display: block;
  font-size: 0.875rem;
  font-weight: 500;
  color: #374151;
  margin-bottom: 0.25rem;
}

.disabled-input {
  width: 100%;
  padding: 0.5rem 1rem;
  border: 1px solid #d1d5db;
  border-radius: 0.5rem;
  background-color: #f9fafb;
  color: #9ca3af;
}

.size-input-group {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.size-input {
  flex: 1;
  max-width: 12rem;
  padding: 0.5rem 1rem;
  border: 1px solid #d1d5db;
  border-radius: 0.5rem;
  outline: none;
  transition: all 0.3s;
}

.size-input:focus {
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.unit-select {
  padding: 0.5rem 1rem;
  border: 1px solid #d1d5db;
  border-radius: 0.5rem;
  background-color: white;
  outline: none;
  transition: all 0.3s;
}

.unit-select:focus {
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.input-hint {
  font-size: 0.75rem;
  color: #6b7280;
  margin-top: 0.25rem;
}

/* 开关组样式 */
.switch-group {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-top: 1rem;
}

.switch-item {
  padding: 0.5rem 0;
}

.switch-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 0.875rem;
  color: #374151;
}

/* 表单操作按钮样式 */
.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 1rem;
  padding-top: 1rem;
  border-top: 1px solid #f3f4f6;
}

.cancel-btn {
  padding: 0.5rem 1rem;
  border: 1px solid #d1d5db;
  color: #374151;
  border-radius: 0.5rem;
  background-color: white;
  cursor: pointer;
  transition: background-color 0.2s;
}

.cancel-btn:hover {
  background-color: #f3f4f6;
}

.save-btn {
  padding: 0.5rem 1rem;
  background-color: #2563eb;
  color: white;
  border-radius: 0.5rem;
  cursor: pointer;
  transition: background-color 0.2s;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
}

.save-btn:hover {
  background-color: #1d4ed8;
  box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
}

/* 提示消息样式 */
.toast {
  position: fixed;
  top: 1rem;
  right: 1rem;
  padding: 0.75rem 1.5rem;
  border-radius: 0.5rem;
  color: white;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  z-index: 50;
  transition: all 0.3s;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
}

.toast.success {
  background-color: #10b981;
}

.toast.error {
  background-color: #ef4444;
}

/* 响应式样式 */
@media (max-width: 768px) {
  .chunk-config-container {
    padding: 1rem;
  }
  
  .config-card {
    padding: 1rem;
  }
  
  .switch-group {
    grid-template-columns: 1fr;
  }
  
  .size-input-group {
    flex-direction: column;
    align-items: stretch;
  }
  
  .size-input {
    max-width: 100%;
  }
}
</style>
