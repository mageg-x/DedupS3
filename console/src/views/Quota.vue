<template>
  <div class="quota-container">
    <!-- 头部 -->
    <div class="storage-points-header mb-6 flex justify-between items-center px-4">
      <div>
        <h2 class="text-2xl font-bold text-gray-800">{{ t('quota.pageTitle') }}</h2>
        <p class="text-gray-500 mt-2">{{ t('quota.pageDescription') }}</p>
      </div>

      <div class="flex gap-3">
        <button v-if="!isEditing" @click="startEditing(null)"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-plus"></i>
          <span>{{ t('quota.addQuota') }}</span>
        </button>
        <button v-else @click="cancelEditing"
          class="px-4 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-times"></i>
          <span>{{ t('quota.cancelEditing') }}</span>
        </button>
      </div>
    </div>

    <!-- 配额列表 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <div v-if="quotas.length === 0" class="text-center py-10">
        <p class="text-gray-500 mb-4">{{ t('quota.noQuotas') }}</p>
        <button @click="startEditing(null)"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2 mx-auto">
          <i class="fas fa-plus"></i>
          <span>{{ t('quota.addQuota') }}</span>
        </button>
      </div>

      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.accountId') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.storageLimit') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.objectLimit') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.status') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="quota in quotas" :key="quota.accountId">
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{{ quota.accountId }}</td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ quota.storageLimit > 0 ? formatStorage(quota.storageLimit) : t('quota.unlimited') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ quota.objectLimit > 0 ? quota.objectLimit.toLocaleString() : t('quota.unlimited') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full"
                  :class="quota.isEnabled ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'">
                  {{ quota.isEnabled ? t('quota.enabled') : t('quota.disabled') }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                <button @click="startEditing(quota)" class="text-blue-600 hover:text-blue-900 mr-3">
                  {{ t('quota.edit') }}
                </button>
                <button @click="handleDelete(quota)" class="text-red-600 hover:text-red-900">
                  {{ t('quota.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 编辑/新增表单 -->
    <div v-if="isEditing" class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <form @submit.prevent="saveQuota" class="space-y-6">
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">
            {{ editingQuota.accountId ? t('quota.edit') : t('quota.addQuota') }}
          </h3>

          <div class="space-y-4">
            <div class="form-field">
              <label for="accountId" class="block text-sm font-medium text-blue-600 mb-1">{{ t('quota.accountId') }} *</label>
              <input
                id="accountId"
                v-model.trim="editingQuota.accountId"
                type="text"
                :placeholder="t('quota.enterAccountId')"
                :disabled="!!editingQuota.accountId"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300"
              />
              <p class="text-xs text-gray-500 mt-1">{{ t('quota.accountIdDesc') }}</p>
            </div>

            <div class="form-field">
              <label class="block text-sm font-medium text-green-600 mb-3">{{ t('quota.quotaConfig') }}</label>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label for="storageLimit" class="block text-sm font-medium text-purple-600 mb-1">{{ t('quota.storageLimit') }}</label>
                  <div class="relative">
                    <input
                      id="storageLimit"
                      v-model.number="editingQuota.storageLimitDisplay"
                      type="number"
                      min="0"
                      placeholder="0"
                      class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300"
                    />
                    <select
                      v-model="editingQuota.storageUnit"
                      class="absolute right-2 top-1/2 transform -translate-y-1/2 border-0 bg-transparent text-gray-500 focus:ring-0"
                    >
                      <option value="KB">KB</option>
                      <option value="MB">MB</option>
                      <option value="GB">GB</option>
                      <option value="TB">TB</option>
                    </select>
                  </div>
                  <p class="text-xs text-gray-500 mt-1">{{ t('quota.zeroMeansUnlimited') }}</p>
                </div>
                <div>
                  <label for="objectLimit" class="block text-sm font-medium text-orange-600 mb-1">{{ t('quota.objectLimit') }}</label>
                  <input
                    id="objectLimit"
                    v-model.number="editingQuota.objectLimit"
                    type="number"
                    min="0"
                    placeholder="0"
                    class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300"
                  />
                  <p class="text-xs text-gray-500 mt-1">{{ t('quota.zeroMeansUnlimited') }}</p>
                </div>
              </div>
            </div>

            <div class="form-field">
              <label class="block text-sm font-medium text-red-600 mb-1">{{ t('quota.statusConfig') }}</label>
              <div class="flex items-center mt-2">
                <label class="relative inline-flex items-center cursor-pointer">
                  <input
                    v-model="editingQuota.isEnabled"
                    type="checkbox"
                    class="sr-only peer"
                  />
                  <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                </label>
                <span class="ml-3 text-sm font-medium text-gray-700">{{ t('quota.enableQuota') }}</span>
              </div>
            </div>
          </div>
        </div>

        <div class="form-actions flex justify-end gap-4 pt-4 border-t border-gray-100">
          <button type="button" @click="cancelEditing"
            class="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors">
            {{ t('quota.cancel') }}
          </button>
          <button type="submit"
            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow-md hover:shadow-lg transition-all duration-300">
            {{ editingQuota.accountId ? t('quota.updateConfig') : t('quota.saveConfig') }}
          </button>
        </div>
      </form>
    </div>

    <!-- 确认对话框 -->
    <div v-if="showConfirmDialog" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50 transition-opacity duration-300">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden transition-all duration-300 transform">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ confirmDialogTitle }}</h3>
          <button @click="closeConfirmDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ confirmDialogMessage }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeConfirmDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('quota.cancel') }}
          </button>
          <button @click="confirmDialogAction" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('quota.confirm') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { listquota, createquota, setquota, delquota } from '../api/admin.js';

export default {
  name: 'Quota',

  // ========== 响应式数据 ==========
  data() {
    return {
      isEditing: false,
      editingQuota: this.createEmptyQuota(),
      quotas: [],
      showConfirmDialog: false,
      confirmDialogTitle: '',
      confirmDialogMessage: '',
      confirmDialogAction: () => {},
    };
  },

  // ========== 生命周期 ==========
  mounted() {
    this.fetchQuotas();
  },

  // ========== 方法 ==========
  methods: {
    // ---------- 工具函数 ----------
    convertToDisplayUnit(kbValue) {
      if (kbValue === 0) return { value: 0, unit: 'KB' };
      const units = ['KB', 'MB', 'GB', 'TB'];
      let value = kbValue;
      let unitIndex = 0;
      while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex++;
      }
      return { value: parseFloat(value.toFixed(2)), unit: units[unitIndex] };
    },

    convertToKB(displayValue, unit) {
      if (displayValue === 0) return 0;
      const multipliers = { KB: 1, MB: 1024, GB: 1024 ** 2, TB: 1024 ** 3 };
      return Math.round(displayValue * multipliers[unit]);
    },

    formatStorage(kbValue) {
      const { value, unit } = this.convertToDisplayUnit(kbValue);
      return `${value} ${unit}`;
    },

    createEmptyQuota() {
      return {
        accountId: '',
        storageLimitDisplay: 0,
        storageUnit: 'GB',
        objectLimit: 0,
        isEnabled: true,
      };
    },

    // ---------- API 调用 ----------
    async fetchQuotas() {
      try {
        const response = await listquota();
        if (response.code === 0 && response.data) {
          this.quotas = response.data.map(item => ({
            accountId: item.accountID || 'default',
            storageLimit: item.maxSpaceSize || 0,
            objectLimit: item.maxObjectCount || 0,
            isEnabled: item.enable || false,
          }));
        } else {
          this.showToastMessage(response.msg || '获取配额失败', 'error');
          this.quotas = [];
        }
      } catch (error) {
        console.error('获取配额失败:', error);
        this.showToastMessage('网络错误，获取配额失败', 'error');
        this.quotas = [];
      }
    },

    async saveQuota() {
      const { accountId, storageLimitDisplay, storageUnit, objectLimit, isEnabled } = this.editingQuota;
      const trimmedId = accountId.trim();

      if (!trimmedId) {
        this.showToastMessage(this.t('quota.enterAccountId'), 'error');
        return;
      }
      if (storageLimitDisplay < 0 || objectLimit < 0) {
        this.showToastMessage('限制值不能为负数', 'error');
        return;
      }

      try {
        const storageLimitInKB = this.convertToKB(storageLimitDisplay, storageUnit);
        const quotaData = {
          accountId: trimmedId,
          MaxSpaceSize: storageLimitInKB,
          MaxObjectCount: objectLimit,
          Enable: isEnabled,
        };

        const response = this.editingQuota.accountId
          ? await setquota(quotaData)
          : await createquota(quotaData);

        if (response.code !== 0) throw new Error(response.msg || '操作失败');

        this.showToastMessage(
          this.editingQuota.accountId ? this.t('quota.updateSuccess') : this.t('quota.addSuccess'),
          'success'
        );

        await this.fetchQuotas();
        this.cancelEditing();
      } catch (error) {
        this.showToastMessage(error.message || '操作失败', 'error');
      }
    },

    async handleDelete(quota) {
      this.confirmDialogTitle = this.t('quota.confirmDeleteTitle');
      this.confirmDialogMessage = this.t('quota.confirmDeleteMessage');
      this.confirmDialogAction = async () => {
        try {
          const response = await delquota({ accountId: quota.accountId });
          if (response.code !== 0) throw new Error(response.msg || '删除失败');
          this.showToastMessage(this.t('quota.deleteSuccess'), 'success');
          await this.fetchQuotas();
        } catch (error) {
          this.showToastMessage(error.message || '删除失败', 'error');
        }
        this.closeConfirmDialog();
      };
      this.showConfirmDialog = true;
    },

    // ---------- UI 控制 ----------
    startEditing(quota) {
      if (quota) {
        const { value, unit } = this.convertToDisplayUnit(quota.storageLimit);
        this.editingQuota = {
          accountId: quota.accountId,
          storageLimitDisplay: value,
          storageUnit: unit,
          objectLimit: quota.objectLimit,
          isEnabled: quota.isEnabled,
        };
      } else {
        this.editingQuota = this.createEmptyQuota();
      }
      this.isEditing = true;
    },

    cancelEditing() {
      this.isEditing = false;
      this.editingQuota = this.createEmptyQuota();
    },

    closeConfirmDialog() {
      this.showConfirmDialog = false;
      this.confirmDialogAction = () => {};
    },

    // ---------- 通用工具 ----------
    showToastMessage(message, type = 'info') {
      const toast = document.createElement('div');
      toast.className = `fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg z-50 transition-opacity duration-300 ${
        type === 'success' ? 'bg-green-500 text-white' : 'bg-red-500 text-white'
      }`;
      toast.innerHTML = `
        <div class="flex items-center gap-2">
          <i class="fas ${type === 'success' ? 'fa-check-circle' : 'fa-exclamation-circle'}"></i>
          <span>${message}</span>
        </div>
      `;
      document.body.appendChild(toast);

      setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => document.body.contains(toast) && document.body.removeChild(toast), 300);
      }, 3000);
    },

    t(key) {
      return this.$t(key);
    },
  },
};
</script>

<style scoped>
.quota-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
}

.storage-points-header {
  margin-bottom: 24px;
}

.config-card {
  background-color: white;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  padding: 24px;
  margin-bottom: 24px;
}

.form-section {
  margin-bottom: 24px;
}

.form-field {
  margin-bottom: 16px;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  padding-top: 16px;
  border-top: 1px solid #f0f0f0;
}

@media (max-width: 768px) {
  .quota-container {
    padding: 16px;
  }

  .config-card {
    padding: 16px;
  }

  .grid-cols-1.md\:grid-cols-2 {
    grid-template-columns: 1fr;
  }
}
</style>